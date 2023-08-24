use crate::compaction::{CompactStatus, KeyRange, LevelCompactStatus, INFO_RANGE};

use crate::level_handler::{LevelHandler, LevelHandlerInner};
use crate::manifest::{Manifest, ManifestChangeBuilder, ManifestFile};
use crate::options::Options;
use crate::pb::badgerpb3::mod_ManifestChange::Operation::{CREATE, DELETE};
use crate::pb::badgerpb3::ManifestChange;
use crate::table::builder::Builder;
use crate::table::iterator::{ConcatIterator, IteratorImpl, IteratorItem};
use crate::table::table::{get_id_map, new_file_name, Table, TableCore};
use crate::types::{Channel, Closer, TArcMx, TArcRW, XArc};
use crate::y::{
    async_sync_directory, create_synced_file, open_existing_synced_file, sync_directory,
};
use crate::Xiterator;
use crate::{hex_str, MergeIterOverBuilder};
use crate::{Result, ValueStruct};
use atomic::Ordering;
use awaitgroup::WaitGroup;
use drop_cell::defer;
use log::{debug, error, info, warn};
use parking_lot::lock_api::RawRwLock;
use tracing::instrument;

use itertools::Itertools;
use rand::random;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::fs::remove_file;
use std::io::Write;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLockReadGuard};
use std::time::{Duration, SystemTime};
use std::vec;
use tokio::macros::support::thread_rng_n;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::time::sleep;

#[derive(Clone)]
pub(crate) struct LevelsController {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandler>>,
    next_file_id: Arc<AtomicU64>,
    // For ending compactions.
    compact_worker_wg: Arc<WaitGroup>,
    // Store compact status that will be run or has running
    c_status: Arc<CompactStatus>,
    manifest: TArcRW<ManifestFile>,
    opt: Options,
    last_unstalled: TArcRW<SystemTime>,
    notify_try_compact_chan: Channel<()>,
    zero_level_compact_chan: Channel<()>,
    notify_write_request_chan: Channel<()>,
}

pub(crate) type XLevelsController = XArc<LevelHandler>;

impl std::fmt::Debug for LevelsController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for level in self.levels.as_ref() {
            level.fmt(f)?;
        }
        Ok(())
    }
}

impl LevelsController {
    pub(crate) async fn new(
        manifest: TArcRW<ManifestFile>,
        notify_try_compact_chan: Channel<()>,
        zero_level_compact_chan: Channel<()>,
        notify_write_request_chan: Channel<()>,
        opt: Options,
    ) -> Result<LevelsController> {
        assert!(opt.num_level_zero_tables_stall > opt.num_level_zero_tables);
        let mut levels = vec![];
        let cstatus = CompactStatus::default();
        for i in 0..opt.max_levels {
            let lh = LevelHandlerInner::new(opt.clone(), i);
            levels.push(LevelHandler::from(lh));
            if i == 0 {
                // Do nothing
            } else if i == 1 {
                // Level 1 probably shouldn't be too much bigger than level 0.
                levels[i]
                    .max_total_size
                    .store(opt.level_one_size, Ordering::Relaxed);
            } else {
                levels[i].max_total_size.store(
                    levels[i - 1].max_total_size.load(Ordering::Relaxed)
                        * opt.level_size_multiplier,
                    Ordering::Relaxed,
                );
            }
            cstatus.wl().push(LevelCompactStatus::default());
        }
        // Compare manifest against directory, check for existent/non-existent files, and remove.
        let mf = manifest.read().await.manifest.clone();
        {
            revert_to_manifest(opt.dir.as_str(), &mf, get_id_map(&opt.dir)).await?;
        }

        // Some files may be deleted. Let's reload.
        let mut tables: Vec<Vec<Table>> = vec![vec![]; levels.len()];
        let mut max_file_id = 0;
        {
            let mf = mf.write().await;
            for (file_id, table_manifest) in &mf.tables {
                let file_name = new_file_name(*file_id, opt.dir.as_str());
                let fd = open_existing_synced_file(&file_name, true)
                    .map_err(|err| format!("Openfile file: {}, err: {}", file_name, err))?;
                let tb = TableCore::open_table(fd, &file_name, opt.table_loading_mode)
                    .map_err(|err| format!("Open file: {}, err :{}", file_name, err))?;
                tables[table_manifest.level as usize].push(Table::new(tb));
                if *file_id > max_file_id {
                    max_file_id = *file_id;
                }
            }
        }

        let next_file_id = max_file_id + 1;
        for (i, tbls) in tables.into_iter().enumerate() {
            levels[i].init_tables(tbls);
        }
        // Make sure key ranges do not overlap etc.
        let level_controller = LevelsController {
            levels: Arc::new(levels),
            next_file_id: Arc::new(AtomicU64::new(next_file_id)),
            compact_worker_wg: Arc::new(Default::default()),
            c_status: Arc::new(cstatus),
            manifest,
            opt: opt.clone(),
            last_unstalled: Arc::new(tokio::sync::RwLock::new(SystemTime::now())),
            notify_try_compact_chan,
            zero_level_compact_chan,
            notify_write_request_chan,
        };
        if let Err(err) = level_controller.validate() {
            let _ = level_controller.cleanup_levels();
            return Err(format!("Level validation, err:{}", err).into());
        }
        // Sync directory (because we have at least removed some files, or previously created the manifest file).
        if let Err(err) = async_sync_directory(*opt.dir.clone()).await {
            let _ = level_controller.close();
            return Err(err);
        }

        Ok(level_controller)
    }

    #[inline]
    pub(crate) fn validate(&self) -> Result<()> {
        for level in self.levels.iter() {
            level.validate()?;
        }
        Ok(())
    }

    pub(crate) fn close(&self) -> Result<()> {
        self.cleanup_levels()
    }

    // returns the found value if any. If not found, we return nil.
    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueStruct> {
        // It's important that we iterate the levels from 0 on upward.  The reason is, if we iterated
        // in opposite order, or in parallel (naively calling all the h.RLock() in some order) we could
        // read level L's tables post-compaction and level L+1's tables pre-compaction.  (If we do
        // parallelize this, we will need to call the h.RLock() function by increasing order of level
        // number.)
        for h in self.levels.iter() {
            h.lock_shared();
            if let Some(item) = h.get(key) {
                #[cfg(test)]
                {
                    if item.key() != key {
                        error!("{} not equal {}", hex_str(key), hex_str(item.key()));
                    }
                    assert_eq!(
                        key,
                        item.key(),
                        "{} not equal {}",
                        hex_str(key),
                        hex_str(item.key())
                    );
                }
                h.unlock_shared();
                return Some(item.value().clone());
            }
            h.unlock_shared();
        }
        None
    }

    // cleanup all level's handler
    fn cleanup_levels(&self) -> Result<()> {
        for level in self.levels.iter() {
            level.close()?;
        }
        Ok(())
    }

    // start compact
    pub(crate) fn start_compact(&self, lc: Closer) {
        for _i in 0..self.opt.num_compactors {
            let lc = lc.spawn();
            let _self = self.clone();
            tokio::spawn(async move {
                _self.run_worker(lc).await;
            });
        }
    }

    // compact worker
    async fn run_worker(&self, lc: Closer) {
        defer! {lc.done()};
        if self.opt.do_not_compact {
            return;
        }

        // random sleep avoid all worker compact at same time
        {
            let duration = thread_rng_n(1000);
            tokio::time::sleep(Duration::from_millis(duration as u64)).await;
        }
        // 1 seconds to check compact
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let zero_level_compact_chan = self.zero_level_compact_chan.tx();
        let notify_try_compact_chan = self.notify_try_compact_chan.rx();
        loop {
            // why interval can life long
            let done = lc.has_been_closed();
            tokio::select! {
                _ = notify_try_compact_chan.recv() => {
                    let pick: Vec<CompactionPriority> = self.pick_compact_levels();
                    info!("Try to compact levels, {:?}", pick);
                    if pick.is_empty() {
                        zero_level_compact_chan.try_send(());
                    }
                    for p in pick {
                        match self.do_compact(p.clone()).await {
                            Ok(true) => {
                                info!("Succeed to compacted");
                                if p.level == 0 {
                                    zero_level_compact_chan.try_send(());
                                }
                            },
                            Ok(false) => {
                                info!("Skip to do compacted");
                                break;
                            },
                            Err(err) => { // TODO handle error
                                error!("Failed to do compacted, {:?}", err);
                            },
                        }
                    }
                    interval.reset();
                },
                _ = interval.tick() => {
                    let pick: Vec<CompactionPriority> = self.pick_compact_levels();
                    info!("Try to compact levels, {:?}", pick);
                    if pick.is_empty() {
                        zero_level_compact_chan.try_send(());
                    }
                    for p in pick {
                        match self.do_compact(p.clone()).await {
                            Ok(true) => {
                                info!("Succeed to compacted");
                                if p.level == 0 {
                                    zero_level_compact_chan.try_send(());
                                }
                            },
                            Ok(false) => {
                                info!("Skip to do compacted");
                                break;
                            },
                            Err(err) => { // TODO handle error
                                error!("Failed to do compacted, {:?}", err);
                            },
                        }
                    }
                },
                _ =  done.recv() => {
                    info!("receive a closer signal for closing compact job");
                    return;
                }
            }
            drop(done);
        }
    }

    // Picks some table on level l and compacts it away to the next level.
    async fn do_compact(&self, p: CompactionPriority) -> Result<bool> {
        let l = p.level;
        assert!(l + 1 < self.opt.max_levels, "Sanity check"); //  Sanity check.

        // merge l's level to (l+1)'s level by p's CompactionPriority
        let mut cd = CompactDef::new(self.levels[l].clone(), self.levels[l + 1].clone());
        info!("Got compaction priority: {:?}", p);
        // While picking tables to be compacted, both level's tables are expected to
        // remain unchanged.
        if l == 0 {
            if !self.fill_tables_l0(&mut cd) {
                info!("failed to fill tables for level {}", l);
                return Ok(false);
            }
        } else {
            if !self.fill_tables(&mut cd) {
                info!(
                    "failed to fill tables for level {}, the compact priority: {:?}",
                    l, p
                );
                return Ok(false);
            }
        }
        let level = cd.this_level.level();
        info!("Running for level: {}", level);
        self.c_status.to_log();
        let cd = TArcRW::new(tokio::sync::RwLock::new(cd));
        let compacted_res = self.run_compact_def(l, cd.clone()).await;
        // delete compact deference information, avoid to cal
        {
            let cd = cd.read().await;
            let cd = cd.deref();
            self.c_status.delete(cd);
        }
        // TODO add clear
        if compacted_res.is_err() {
            error!(
                "LOG Compact FAILED with error: {}",
                compacted_res.unwrap_err().to_string()
            );
        }
        // Done with compaction. So, remove the ranges from compaction status.
        info!("Compaction for level: {} DONE", level);
        Ok(true)
    }

    /// Handle compact deference
    async fn run_compact_def(&self, l: usize, cd: Arc<RwLock<CompactDef>>) -> Result<()> {
        let time_start = SystemTime::now();
        let this_level = cd.read().await.this_level.clone();
        let next_level = cd.read().await.next_level.clone();

        {
            let cd = cd.read().await;
            if this_level.level() >= 1 && cd.bot.is_empty() {
                assert_eq!(cd.top.len(), 1);
                let table_lck = cd.top[0].clone();
                // We write to the manifest _before_ we delete files (and after we created files).
                // The order matters here -- you can't temporarily have two copies of the same
                // table id when reloading the manifest.
                // TODO Why?
                let delete_change = ManifestChangeBuilder::new(table_lck.id())
                    .with_op(DELETE)
                    .build();
                let create_change = ManifestChangeBuilder::new(table_lck.id())
                    .with_level(next_level.level() as u32)
                    .with_op(CREATE)
                    .build();
                let changes = vec![delete_change, create_change];
                let mut manifest = self.manifest.write().await;
                manifest.add_changes(changes).await?;
                // We have to add to next_level before we remove from this_level, not after. This way, we
                // don't have a bug where reads would see keys missing from both levels.
                //
                // Note: It's critical that we add tables (replace them) in next_level before deleting them
                // in this_level. (We could finagle it atomically somehow.) Also, when reading we must
                // read, or at least acquire s.rlock(), in increasing order by level, so that we don't skip
                // a compaction.
                next_level.replace_tables(cd.top.clone())?;
                let top_ids = cd.top.iter().map(|tb| tb.id()).collect::<Vec<_>>();
                this_level.delete_tables(top_ids);
                info!(
                    "LOG Compact-Move {}->{} smallest:{} biggest:{} took {}",
                    l,
                    l + 1,
                    hex_str(table_lck.smallest()),
                    hex_str(table_lck.biggest()),
                    time_start.elapsed().unwrap().as_millis(),
                );
                return Ok(());
            }
        }

        // NOTE: table deref
        let new_tables = self.compact_build_tables(l, cd.clone()).await?;
        let deref_tables = || new_tables.iter().for_each(|tb| tb.decr_ref());
        defer! {deref_tables();}

        // TODO add a change commit
        info!("manifest file {:?}", self.opt);
        let cd = cd.write().await;
        let change_set = Self::build_change_set(&cd, &new_tables);

        // We write to the manifest _before_ we delete files (and after we created files)
        {
            let mut manifest = self.manifest.write().await;
            manifest.add_changes(change_set).await?;
        }

        // See comment earlier in this function about the ordering of these ops, and the order in which
        // we access levels whe reading.
        next_level.replace_tables(new_tables.clone())?;
        let top_ids = cd.top.iter().map(|tb| tb.id()).collect::<Vec<_>>();
        this_level.delete_tables(top_ids);

        // Note: For level 0, while do_compact is running, it is possible that new tables are added.
        // However, the tables are added only to the end, so it is ok to just delete the first table.
        info!(
            "LOG Compact {}->{}, del {} tables, add {} tables, took {}ms",
            l,
            l + 1,
            cd.top.len() + cd.bot.len(),
            new_tables.len(),
            time_start.elapsed().unwrap().as_millis()
        );
        info!("this level: {:?}", this_level.to_log());
        info!("next level: {:?}", next_level.to_log());
        Ok(())
    }

    // async to add level0 table
    pub(crate) async fn add_level0_table(&self, table: Table) -> Result<()> {
        defer! {info!("Finish add level0 table")}
        // We update the manifest _before_ the table becomes part of a levelHandler, because at that
        // point it could get used in some compaction.  This ensures the manifest file gets updated in
        // the proper order. (That means this update happens before that of some compaction which
        // deletes the table.)
        self.manifest
            .write()
            .await
            .add_changes(vec![ManifestChangeBuilder::new(table.id())
                .with_level(0)
                .with_op(CREATE)
                .build()])
            .await?;
        info!("Ready add level0 table, id:{}", table.id());
        let zero_level_compact_chan = self.zero_level_compact_chan.rx();
        let notify_try_compact_chan = self.notify_try_compact_chan.tx();
        while !self.levels[0].try_add_level0_table(table.clone()).await {
            // Notify compact job
            notify_try_compact_chan.try_send(());
            // Stall. Make sure all levels are healthy before we unstall.
            let mut start_time = SystemTime::now();
            {
                info!(
                    "STALLED STALLED STALLED STALLED STALLED STALLED STALLED STALLED: {}ms",
                    self.last_unstalled
                        .read()
                        .await
                        .elapsed()
                        .unwrap()
                        .as_millis()
                );
                info!("{:?}, {}", self.opt, self.levels[0].num_tables());
                let c_status = self.c_status.rl();
                for i in 0..self.opt.max_levels {
                    info!(
                        "level={}, status={}, size={}",
                        i,
                        c_status[i],
                        c_status[i].get_del_size()
                    )
                }
                start_time = SystemTime::now();
            }
            // Before we unstall, we need to make sure that level 0 and 1 are healthy. Otherwise, we
            // will very quickly fill up level 0 again and if the compaction strategy favors level 0,
            // then level 1 is going to super full.
            loop {
                // Passing 0 for delSize to compactable means we're treating incomplete compactions as
                // not having finished -- we wait for them to finish.  Also, it's crucial this behavior
                // replicates pickCompactLevels' behavior in computing compactability in order to
                // guarantee progress.
                // TODO why level1 delesize set to zero
                // let del_size = self.c_status.del_size(1);
                let del_size = 0;
                if !self.is_level0_compactable() && !self.levels[1].is_compactable(del_size) {
                    break;
                }
                tokio::select! {
                     _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                    _ = zero_level_compact_chan.recv() => {
                        info!("receive a continue event");
                    },
                }
                debug!(
                    "Try again to check level0 compactable, Waitting gc job compact level zero SST"
                );
            }

            info!(
                "UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED UNSTALLED: {}ms",
                start_time.elapsed().unwrap().as_millis()
            );
            *self.last_unstalled.write().await = SystemTime::now();
        }
        let _ = self.notify_write_request_chan.tx().try_send(());
        Ok(())
    }

    pub(crate) fn as_iterator(
        &self,
        reverse: bool,
    ) -> Vec<Box<dyn Xiterator<Output = IteratorItem>>> {
        let mut itrs: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
        for level in self.levels.iter() {
            if level.level() == 0 {
                for table in level.tables.read().iter().rev() {
                    let itr = Box::new(IteratorImpl::new(table.clone(), reverse));
                    itrs.push(itr);
                }
            } else {
                for table in level.tables.read().iter() {
                    let itr = Box::new(IteratorImpl::new(table.clone(), reverse));
                    itrs.push(itr);
                }
            }
        }
        itrs
    }

    // Merge top tables and bot tables to from a List of new tables.
    #[instrument(skip(self))]
    pub(crate) async fn compact_build_tables(
        &self,
        l: usize,
        cd: TArcRW<CompactDef>,
    ) -> Result<Vec<Table>> {
        info!("Start compact build tables");
        defer! {info!("Finish compact build tables")}
        // Start generating new tables.
        let (tx, mut rv) = tokio::sync::mpsc::unbounded_channel::<Result<Table>>();
        let mut g = WaitGroup::new();
        let execute_time = SystemTime::now();
        defer! {
            let cost = SystemTime::now().duration_since(execute_time).unwrap().as_millis();
            crate::event::COMPACT_COST_TIME.inc_by(cost as u64);
        }
        {
            let cd = cd.read().await;
            let mut top_tables = cd.top.clone();
            let bot_tables = cd.bot.clone();
            // Create iterators across all the tables involved first.
            let mut itr: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
            if l == 0 {
                top_tables.reverse();
            } else {
                assert_eq!(1, top_tables.len());
            }
            let is_empty = bot_tables.is_empty();
            for tb in top_tables {
                let iter = Box::new(IteratorImpl::new(tb, false));
                itr.push(iter);
            }
            // Next level has level>=1, so we can use ConcatIterator as key ranges do not overlap.
            let citr = ConcatIterator::new(bot_tables, false);
            itr.push(Box::new(citr));
            let mitr = MergeIterOverBuilder::default().add_batch(itr).build();
            // Important to close the iterator to do ref counting.
            defer! {mitr.close()}
            {
                mitr.rewind();
                // mitr.export_disk();
                mitr.export_disk_ext();
            }
            mitr.rewind();
            let tid = random::<u32>();
            let mut count = 0;
            let cur = tokio::runtime::Handle::current();
            loop {
                #[cfg(test)]
                let mut keys = vec![];
                let start_time = SystemTime::now();
                let mut builder = Builder::default();
                while let Some(value) = mitr.peek() {
                    count += 1;
                    assert!(builder.add(value.key(), value.value()).is_ok());
                    mitr.next();
                    if builder.reached_capacity(self.opt.max_table_size) {
                        break;
                    }

                    #[cfg(test)]
                    {
                        info!("merge, mitr{}, key {}", mitr.id(), hex_str(value.key()));
                        {
                            crate::test_util::push_log(
                                format!(
                                    "tid:{}, mitr:{}, key:{}",
                                    tid,
                                    mitr.id(),
                                    hex_str(value.key())
                                )
                                .as_bytes(),
                                false,
                            );
                        }
                        keys.push(value.key().to_vec());
                    }
                }
                if builder.is_zero_bytes() {
                    warn!("Builder is empty");
                    break;
                }

                let file_id = self.reserve_file_id();
                // It was true that it.Valid() at least once in the loop above, which means we
                // called Add() at least once, and builder is not Empty().
                info!(
                    "LOG Compacted: Iteration to generate one table [{}] took: {}ms",
                    file_id,
                    start_time.elapsed().unwrap().as_millis()
                );

                let dir = self.opt.dir.clone();
                let file_name = new_file_name(file_id, &dir);
                #[cfg(test)]
                {
                    let str = keys.into_iter().map(|key| hex_str(&key)).join(",");
                    debug!("Save keys into {} {}", file_name, str);
                }
                let worker = g.worker();
                let tx = tx.clone();
                let loading_mode = self.opt.table_loading_mode;
                tokio::spawn(async move {
                    defer! {worker.done();}
                    let fd = create_synced_file(&file_name, true);
                    if let Err(err) = fd {
                        tx.send(Err(format!(
                            "While opening new table: {}, err: {}",
                            file_id, err
                        )
                        .into()))
                            .unwrap();
                        return;
                    }
                    if let Err(err) = fd.as_ref().unwrap().write_all(&builder.finish()) {
                        tx.send(Err(format!(
                            "Unable to write to file: {}, err: {}",
                            file_id, err
                        )
                        .into()))
                            .unwrap();
                        return;
                    }
                    let tbl = TableCore::open_table(fd.unwrap(), &file_name, loading_mode);
                    if let Err(err) = tbl {
                        tx.send(Err(format!(
                            "Unable to open table: {}, err: {}",
                            file_name, err
                        )
                        .into()))
                            .unwrap();
                    } else {
                        tx.send(Ok(Table::new(tbl.unwrap()))).unwrap();
                    }
                });
            }
        }
        g.wait().await;
        drop(tx);
        info!(
            "Compacted took {}ms",
            execute_time.elapsed().unwrap().as_millis()
        );

        let mut new_tables = Vec::with_capacity(20);
        let mut first_err: Result<()> = Ok(());
        // Wait for all table builders to finished.
        loop {
            let tb = rv.recv().await;
            if tb.is_none() {
                break;
            }
            match tb.unwrap() {
                Ok(tb) => {
                    info!("Create a new table, fid: {}", tb.id());
                    new_tables.push(tb);
                }
                Err(err) => {
                    error!("{}", err);
                    if first_err.is_ok() {
                        first_err = Err(err);
                    }
                }
            }
        }
        if first_err.is_ok() {
            // Ensure created files' directory entries are visible, We don't mind the extra latency
            // from not doing this ASAP after all file creation has finished because this is a
            // background operation
            first_err = sync_directory(&self.opt.dir);
        }
        // sort asc by table's biggest
        new_tables.sort_by(|a, b| a.to_ref().biggest().cmp(b.to_ref().biggest()));
        if first_err.is_err() {
            // An error happened. Delete all the newly created table files (by calling Decref
            // -- we're the only holders of a ref).
            let _ = new_tables.iter().map(|tb| tb.decr_ref());
            return Err(format!(
                "While running compaction for: {}, err: {}",
                cd.read().await,
                first_err.unwrap_err()
            )
            .into());
        }
        Ok(new_tables)
    }

    fn build_change_set(cd: &CompactDef, new_tables: &Vec<Table>) -> Vec<ManifestChange> {
        // new tables to CREATE
        let mut changes = new_tables
            .iter()
            .map(|tb| {
                ManifestChangeBuilder::new(tb.id())
                    .with_level(cd.next_level.level() as u32)
                    .with_op(CREATE)
                    .build()
            })
            .collect::<Vec<_>>();
        // top compact to DELETE
        changes.extend(
            cd.top
                .iter()
                .map(|tb| ManifestChangeBuilder::new(tb.id()).with_op(DELETE).build()),
        );
        // bot compact to DELETE
        changes.extend(
            cd.bot
                .iter()
                .map(|tb| ManifestChangeBuilder::new(tb.id()).with_op(DELETE).build()),
        );

        changes
    }

    // Find the KeyRange that should be merged between cd.this and cd.next
    fn fill_tables_l0(&self, cd: &mut CompactDef) -> bool {
        // lock this, next levels avoid other GC worker
        cd.lock_exclusive_levels();
        let top = cd.this_level.to_ref().tables.read();
        // TODO here maybe have some issue that i don't understand
        let tables = top.to_vec();
        // all tables at zero table will be merge level1
        #[cfg(test)]
        assert!(cd.top.is_empty(), "Sanity check!");
        cd.top.extend(tables);
        if cd.top.is_empty() {
            cd.unlock_exclusive_levels();
            return false;
        }
        // all kv pair has included.
        cd.this_range = INFO_RANGE;
        let kr = KeyRange::get_range(cd.top.as_ref());
        let (left, right) = cd.next_level.overlapping_tables(&kr);
        let bot = cd.next_level.to_ref().tables.read();
        let tables = bot.to_vec();
        // fill bottom (next level) tables
        cd.bot.extend(tables[left..right].to_vec());
        if cd.bot.is_empty() {
            // not found any tables. Just fill top tables is ok !
            cd.next_range = kr;
        } else {
            cd.next_range = KeyRange::get_range(cd.bot.as_ref());
        }
        // Add compact status avoid other gcc operate the `RangeKeys`
        if !self.c_status.compare_and_add(cd) {
            cd.unlock_exclusive_levels();
            return false;
        }
        cd.unlock_exclusive_levels();
        true
    }

    // fill tables for compactDef, and locked them KeyRange
    fn fill_tables(&self, cd: &mut CompactDef) -> bool {
        // lock current level and next levels, So there is at most one compression process per layer
        cd.lock_exclusive_levels();
        let mut tables = cd.this_level.to_ref().tables.read().to_vec();
        if tables.is_empty() {
            info!("the tables is empty, skip compact deference");
            cd.unlock_exclusive_levels();
            return false;
        }
        // Find the biggest table, and compact that first.
        // TODO: Try other table picking strategies.
        tables.sort_by(|a, b| b.size().cmp(&a.size()));
        for t in tables {
            let this_range = KeyRange {
                left: t.smallest().to_vec(),
                right: t.biggest().to_vec(),
                inf: false,
            };
            if self
                .c_status
                .overlaps_with(cd.this_level.level(), &this_range)
            {
                info!(
                    "not found overlaps with this range: {}",
                    this_range.to_string()
                );
                continue;
            }
            cd.this_size.store(t.size() as u64, Ordering::Relaxed);
            cd.this_range = this_range;
            {
                cd.top.clear();
                cd.top.push(t);
            }

            // Find next overlap that will be compacted
            // TODO [left, right)
            let (left, right) = cd.next_level.overlapping_tables(&cd.this_range);
            let bot = cd.next_level.to_ref().tables.read();
            let tables = bot.to_vec();
            {
                cd.bot.clear();
                cd.bot.extend(tables[left..right].to_vec());
            }

            // not find any overlap at next levels, so sample insert it
            if cd.bot.is_empty() {
                cd.next_range = cd.this_range.clone();
                if !self.c_status.compare_and_add(cd) {
                    info!("find a conflict compacted, cd: {}", cd);
                    continue;
                }
                cd.unlock_exclusive_levels();
                return true;
            }

            cd.next_range = KeyRange::get_range(cd.bot.as_ref());

            if self
                .c_status
                .overlaps_with(cd.next_level.level(), &cd.next_range)
            {
                info!("find a conflict compacted: {}", cd);
                continue;
            }

            if !self.c_status.compare_and_add(&cd) {
                info!("failed to compactDef to c_status, {}", cd);
                continue;
            }
            cd.unlock_exclusive_levels();
            return true;
        }
        cd.unlock_exclusive_levels();
        false
    }

    // Determines which level to compact.
    // Base on https://github.com/facebook/rocksdb/wiki/Leveled-Compaction.
    #[instrument(skip(self))]
    fn pick_compact_levels(&self) -> Vec<CompactionPriority> {
        // This function must use identical criteria for guaranteeing compaction's progress that
        // add_level0_table use.

        let mut prios = vec![];
        // c_status is checked to see if level 0's tables are already being compacted.
        // *NOTICE* level 0 only has one compact job
        if !self.c_status.overlaps_with(0, &INFO_RANGE) && self.is_level0_compactable() {
            prios.push(CompactionPriority {
                level: 0,
                score: (self.levels[0].num_tables() as f64)
                    / (self.opt.num_level_zero_tables as f64),
            });
            info!("level0 will be compacted");
        }
        // stats level 1..n
        for (i, level) in self.levels[1..].iter().enumerate() {
            // Don't consider those tables that are already being compacted right now.
            // let del_size = self.c_status.del_size(i + 1);
            let del_size = 0;
            if level.is_compactable(del_size) {
                prios.push(CompactionPriority {
                    level: i + 1,
                    score: ((level.get_total_size() - del_size) as f64
                        / level.get_max_total_size() as f64),
                });
            }
        }
        // sort from big to small.
        prios.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        prios
    }

    // Return true if level zero may be compacted, without accounting for compactions that already
    // might be happening.
    fn is_level0_compactable(&self) -> bool {
        let compactable = self.levels[0].num_tables() >= self.opt.num_level_zero_tables;
        #[cfg(test)]
        debug!(
            "level0 compactable, num_tables: {}, config_tables: {}, yes: {}",
            self.levels[0].num_tables(),
            self.opt.num_level_zero_tables,
            compactable
        );

        compactable
    }

    // calc next file id
    pub(crate) fn reserve_file_id(&self) -> u64 {
        let id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        id
    }

    pub(crate) fn print_level_fids(&self) {
        let sz = self
            .levels
            .iter()
            .map(|lv| lv.num_tables())
            .collect::<Vec<_>>();
        warn!("every level table's size: {:?}", sz);
    }
}

#[derive(Debug, Clone)]
struct CompactionPriority {
    level: usize,
    score: f64,
}

// Compact deference
pub(crate) struct CompactDef {
    pub(crate) this_level: LevelHandler,
    pub(crate) next_level: LevelHandler,
    pub(crate) top: Vec<Table>,
    // if the level is not level0, it should be only one table
    pub(crate) bot: Vec<Table>,
    // may be empty tables set
    pub(crate) this_range: KeyRange,
    pub(crate) next_range: KeyRange,
    pub(crate) this_size: AtomicU64, // the compacted table's size(NOTE: this level compacted table is only one, exclude zero level)
}

impl Debug for CompactDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for CompactDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let top = self
            .top
            .iter()
            .map(|table| table.id().to_string())
            .collect::<Vec<_>>();
        let bot = self
            .bot
            .iter()
            .map(|table| table.id().to_string())
            .collect::<Vec<_>>();
        write!(
            f,
            "(this_level: {}, next_level: {}, this_sz: {}, top: {:?}, bot: {:?})",
            self.this_level.level(),
            self.next_level.level(),
            self.this_size.load(Ordering::Relaxed),
            top,
            bot
        )
    }
}

impl CompactDef {
    pub(crate) fn new(this_level: LevelHandler, next_level: LevelHandler) -> Self {
        CompactDef {
            this_level,
            next_level,
            top: vec![],
            bot: vec![],
            this_range: KeyRange::default(),
            next_range: KeyRange::default(),
            this_size: Default::default(),
        }
    }

    #[inline]
    fn lock_shared_levels(&self) {
        self.this_level.lock_shared();
        self.next_level.lock_shared();
    }

    #[inline]
    fn unlock_shared_levels(&self) {
        self.next_level.unlock_shared();
        self.this_level.unlock_shared();
    }

    #[inline]
    pub fn lock_exclusive_levels(&self) {
        self.this_level.lock_exclusive();
        self.next_level.lock_exclusive();
    }

    #[inline]
    pub fn unlock_exclusive_levels(&self) {
        self.next_level.unlock_exclusive();
        self.this_level.unlock_exclusive();
    }
}

// Checks that all necessary table files exist and removes all table files not
// referenced by the manifest. id_map is a set of table file id's that were read from the directory
// listing.
async fn revert_to_manifest(dir: &str, mf: &TArcRW<Manifest>, id_map: HashSet<u64>) -> Result<()> {
    let tables = mf.write().await;
    // 1. Check all files in manifest exist.
    for id in &tables.tables {
        if !id_map.contains(id.0) {
            return Err(format!("file does not exist for table {}", id.0).into());
        }
    }

    // 2. Delete files that shouldn't exist.
    for id in &id_map {
        if !tables.tables.contains_key(id) {
            error!("table file {} not referenced in MANIFEST", id);
            let file_name = new_file_name(*id, dir);
            if let Err(err) = remove_file(file_name) {
                error!("While removing table {}, err: {}", id, err);
            }
        }
    }
    Ok(())
}

#[test]
fn it() {}
