use crate::compaction::{CompactStatus, KeyRange, INFO_RANGE};
use crate::kv::{ArcKV, WeakKV, KV};
use crate::level_handler::{LevelHandler, LevelHandlerInner};
use crate::manifest::{Manifest, ManifestChangeBuilder, ManifestFile};
use crate::options::Options;
use crate::pb::badgerpb3::manifest_change::Operation::{CREATE, DELETE};
use crate::pb::badgerpb3::{ManifestChange, ManifestChangeSet};
use crate::table::builder::Builder;
use crate::table::iterator::{ConcatIterator, IteratorItem};

use crate::table::table::{new_file_name, Table, TableCore};
use crate::types::{Channel, Closer, XArc, XWeak};
use crate::y::iterator::{MergeIterOverBuilder, Xiterator};
use crate::Error::Unexpected;
use crate::Result;
use atomic::Ordering;
use awaitgroup::WaitGroup;
use drop_cell::defer;
use log::{error, info};
use parking_lot::lock_api::RawRwLock;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::fs::remove_file;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::vec;
use tokio::macros::support::thread_rng_n;

#[derive(Clone)]
pub(crate) struct LevelsController {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandler>>,
    kv: WeakKV,
    next_file_id: Arc<AtomicI64>,
    // For ending compactions.
    compact_worker_wg: Arc<WaitGroup>,
    // Store compact status that will be run or has running
    c_status: Arc<CompactStatus>,
}

unsafe impl Sync for LevelsController {}

unsafe impl Send for LevelsController {}

impl Default for LevelsController {
    fn default() -> Self {
        todo!()
    }
}

impl LevelsController {
    fn new(kv: ArcKV, mf: &Manifest) -> Result<LevelsController> {
        assert!(kv.x.opt.num_level_zero_tables_stall > kv.x.opt.num_level_zero_tables);
        let mut levels = vec![];
        for i in 0..kv.x.opt.max_levels {
            let lh = LevelHandlerInner::new(WeakKV::from(&kv), i);
            levels.push(LevelHandler::from(lh));
            if i == 0 {
            } else if i == 1 {
            } else {
            }
        }
        todo!()
    }

    // cleanup all level's handler
    fn cleanup_levels(&self) -> Result<()> {
        for level in self.levels.iter() {
            level.close()?;
        }
        Ok(())
    }

    // start compact
    fn start_compact(&self, lc: Closer) {
        for i in 0..self.must_kv().opt.num_compactors {
            let lc = lc.spawn();
            let _self = self.clone();
            tokio::spawn(async move {
                _self.run_worker(lc).await;
            });
        }
    }

    // compact worker
    async fn run_worker(&self, lc: Closer) {
        if self.must_kv().opt.do_not_compact {
            lc.done();
            return;
        }
        // random sleep avoid all worker compact at same time
        {
            let duration = thread_rng_n(1000);
            tokio::time::sleep(Duration::from_millis(duration as u64)).await;
        }
        // 1 seconds to check compact
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            // why interval can life long
            let done = lc.has_been_closed();
            tokio::select! {
                _ = interval.tick() => {
                    let pick: Vec<CompactionPriority> = self.pick_compact_levels();
                    for p in pick {
                        match self.do_compact(p).await {
                            Ok(true) => {
                                info!("succeed to compacted")
                            },
                            Ok(false) => {
                                info!("skip to do compacted");
                                break;
                            },
                            Err(err) => { // TODO handle error
                                error!("failed to do compacted, {:?}", err);
                            },
                        }
                    }
                },
                _ =  done.recv() => {
                    info!("receive a closer signal for closing compact job");
                    return;
                }
            }
        }
    }

    // Picks some table on level l and compacts it away to the next level.
    async fn do_compact(&self, p: CompactionPriority) -> Result<bool> {
        let l = p.level;
        assert!(l + 1 < self.must_kv().opt.max_levels); //  Sanity check.
        let mut cd = CompactDef::default();
        cd.this_level = (self.levels[l]).clone();
        cd.next_level = (self.levels[l + 1]).clone();
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
                info!("failed to fill tables for level {}", l);
                return Ok(false);
            }
        }
        let level = cd.this_level.level();
        info!("Running for level: {}", level);
        info!("{:?}", self.c_status);
        let compacted_res = self.run_compact_def(l, cd).await;
        if compacted_res.is_err() {
            error!(
                "LOG Compact FAILED with error: {}",
                compacted_res.unwrap_err().to_string()
            );
        }
        // Done with compaction. So, remove the ranges from compaction status.
        self.c_status.del_size(level);
        info!("Compaction for level: {} DONE", level);
        Ok(true)
    }

    async fn run_compact_def(&self, l: usize, cd: CompactDef) -> Result<()> {
        let time_start = SystemTime::now();
        let this_level = cd.this_level.clone();
        let next_level = cd.next_level.clone();

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
            let kv = self.must_kv();
            let mut manifest = kv.manifest.write().await;
            manifest.add_changes(changes)?;
            // We have to add to next_level before we remove from this_level, not after. This way, we
            // don't have a bug where reads would see keys missing from both levels.
            //
            // Note: It's critical that we add tables (replace them) in next_level before deleting them
            // in this_level. (We could finagle it atomically somehow.) Also, when reading we must
            // read, or at least acquire s.rlock(), in increasing order by level, so that we don't skip
            // a compaction.
            next_level.replace_tables(cd.top.clone())?;
            this_level.replace_tables(cd.top.clone())?;
            info!(
                "LOG Compact-Move {}->{} smallest:{} biggest:{} took {}",
                l,
                l + 1,
                String::from_utf8_lossy(table_lck.smallest()),
                String::from_utf8_lossy(table_lck.biggest()),
                time_start.elapsed().unwrap().as_millis(),
            );
            return Ok(());
        }

        let cd = Arc::new(tokio::sync::RwLock::new(cd));
        // NOTE: table deref
        let new_tables = self.compact_build_tables(l, cd.clone()).await?;
        let deref_tables = || new_tables.iter().for_each(|tb| tb.decr_ref());
        defer! {deref_tables();}

        let cd = cd.write().await;
        let change_set = Self::build_change_set(&cd, &new_tables);

        // We write to the manifest _before_ we delete files (and after we created files)
        {
            let kv = self.must_kv();
            let mut manifest = kv.manifest.write().await;
            manifest.add_changes(change_set)?;
        }

        // See comment earlier in this function about the ordering of these ops, and the order in which
        // we access levels whe reading.
        next_level.replace_tables(new_tables.clone())?;
        this_level.replace_tables(cd.top.clone())?;

        // Note: For level 0, while do_compact is running, it is possible that new tables are added.
        // However, the tables are added only to the end, so it is ok to just delete the first table.
        info!(
            "LOG Compact {}->{}, del {} tables, add {} tables, took {}",
            l,
            l + 1,
            cd.top.len() + cd.bot.len(),
            new_tables.len(),
            time_start.elapsed().unwrap().as_millis()
        );

        Ok(())
    }

    // Merge top tables and bot tables to from a List of new tables.
    pub(crate) async fn compact_build_tables(
        &self,
        l: usize,
        cd: Arc<tokio::sync::RwLock<CompactDef>>,
    ) -> Result<Vec<Table>> {
        // Start generating new tables.
        struct NewTableResult {
            table: Table,
            err: Result<()>,
        }
        let result_ch: Channel<NewTableResult> = Channel::new(1);
        {
            let cd = cd.read().await;
            let top_tables = cd.top.clone();
            let bot_tables = cd.bot.clone();
            // Create iterators across all the tables involved first.
            let mut itr: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
            if l == 0 {
                // Self::append_iterators_reversed(&mut itr, &top_tables, false);
            } else {
                assert_eq!(1, top_tables.len());
                // Self::append_iterators_reversed(&mut itr, &top_tables[..1].to_vec(), false);
            }

            // Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
            // TODO
            let citr = ConcatIterator::new(bot_tables, false);
            itr.push(Box::new(citr));
            // let mitr = MergeIterOverBuilder::default().add_batch(itr).build();
            let mitr = MergeIterOverBuilder::default().build();

            // Important to close the iterator to do ref counting.
            defer! {mitr.close()};
            mitr.rewind();
            loop {
                let start_time = SystemTime::now();
                let mut builder = Builder::default();
                for value in mitr.next() {
                    if builder.reached_capacity(self.must_kv().opt.max_table_size) {
                        break;
                    }
                    assert!(builder.add(value.key(), value.value()).is_ok());
                }
                if builder.empty() {
                    break;
                }
                // It was true that it.Valid() at least once in the loop above, which means we
                // called Add() at least once, and builder is not Empty().
                info!(
                    "LOG Compacted: Iteration to generate one table took: {}",
                    start_time.elapsed().unwrap().as_millis()
                );

                // TODO
                let file_id = self.reserve_file_id();
                // async
            }
        }

        let mut new_tables = vec![];
        let mut first_err = Ok(());
        // Wait for all table builders to finished.
        while let Ok(ret) = result_ch.recv().await {
            new_tables.push(ret.table.clone());
            if ret.err.is_err() {
                first_err = ret.err;
            }
        }
        todo!()
    }

    // TODO
    fn append_iterators_reversed(
        out: &mut Vec<Box<dyn Xiterator<Output = IteratorItem>>>,
        th: &Vec<Table>,
        reversed: bool,
    ) {
        // for itr_th in th.iter().rev() {
        //     // This will increment the reference of the table handler.
        //     let itr = IteratorImpl::new(itr_th, reversed);
        //     out.push(Box::new(itr));
        // }
    }

    fn build_change_set(cd: &CompactDef, new_tables: &Vec<Table>) -> Vec<ManifestChange> {
        let mut changes = vec![];
        for table in new_tables {
            changes.push(
                ManifestChangeBuilder::new(table.id())
                    .with_level(cd.next_level.level() as u32)
                    .with_op(CREATE)
                    .build(),
            );
        }

        for table in cd.top.iter() {
            changes.push(
                ManifestChangeBuilder::new(table.id())
                    .with_op(DELETE)
                    .build(),
            );
        }

        for table in cd.bot.iter() {
            changes.push(
                ManifestChangeBuilder::new(table.id())
                    .with_op(DELETE)
                    .build(),
            );
        }

        changes
    }

    fn fill_tables_l0(&self, cd: &mut CompactDef) -> bool {
        cd.lock_shared_levels();
        let top = cd.this_level.to_ref().tables.read();
        // TODO here maybe have some issue that i don't understand
        let tables = top.to_vec();
        cd.top.extend(tables);
        if cd.top.is_empty() {
            cd.unlock_shared_levels();
            return false;
        }
        cd.this_range = INFO_RANGE;
        let kr = KeyRange::get_range(cd.top.as_ref());
        let (left, right) = cd.next_level.overlapping_tables(&kr);
        let bot = cd.next_level.to_ref().tables.read();
        let tables = bot.to_vec();
        cd.bot.extend(tables[left..right].to_vec());
        if cd.bot.is_empty() {
            cd.next_range = kr;
        } else {
            cd.next_range = KeyRange::get_range(cd.bot.as_ref());
        }
        if !self.c_status.compare_and_add(cd) {
            return false;
        }
        cd.unlock_shared_levels();
        true
    }

    fn fill_tables(&self, cd: &mut CompactDef) -> bool {
        // lock current level and next levels, So there is at most one compression process per layer
        cd.lock_shared_levels();
        let mut tables = cd.this_level.to_ref().tables.read().to_vec();
        if tables.is_empty() {
            cd.unlock_shared_levels();
            return false;
        }
        // Find the biggest table, and compact that first.
        // TODO: Try other table picking strategies.
        tables.sort_by(|a, b| b.size().cmp(&a.size()));
        for t in tables {
            cd.this_size.store(t.size() as u64, Ordering::Relaxed);
            cd.this_range = KeyRange {
                left: t.smallest().to_vec(),
                right: t.biggest().to_vec(),
                inf: false,
            };
            if self
                .c_status
                .overlaps_with(cd.this_level.level(), &cd.this_range)
            {
                continue;
            }

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
                cd.unlock_shared_levels();
                return true;
            }

            cd.next_range = KeyRange::get_range(cd.bot.as_ref());

            if self
                .c_status
                .overlaps_with(cd.next_level.level(), &cd.next_range)
            {
                continue;
            }

            if !self.c_status.compare_and_add(&cd) {
                continue;
            }
            cd.unlock_shared_levels();
            return true;
        }
        cd.unlock_shared_levels();
        false
    }

    // Determines which level to compact.
    // Base on https://github.com/facebook/rocksdb/wiki/Leveled-Compaction.
    fn pick_compact_levels(&self) -> Vec<CompactionPriority> {
        // This function must use identical criteria for guaranteeing compaction's progress that
        // add_level0_table use.

        let mut prios = vec![];
        // cstatus is checked to see if level 0's tables are already being compacted.
        // *NOTICE* level 0 only has one compact job
        if !self.c_status.overlaps_with(0, &INFO_RANGE) && self.is_level0_compactable() {
            prios.push(CompactionPriority {
                level: 0,
                score: (self.levels[0].num_tables() as f64)
                    / (self.must_kv().opt.num_level_zero_tables as f64),
            })
        }

        // stats level 1..n
        for (i, level) in self.levels[1..].iter().enumerate() {
            // Don't consider those tables that are already being compacted right now.
            let del_size = self.c_status.del_size(i + 1);
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
        self.levels[0].num_tables() >= self.must_kv().opt.num_level_zero_tables
    }

    fn must_kv(&self) -> Arc<KV> {
        self.kv.x.upgrade().unwrap()
    }

    fn reserve_file_id(&self) -> i64 {
        let id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        id
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
    pub(crate) top: Vec<Table>, // if the level is not level0, it should be only one table
    pub(crate) bot: Vec<Table>, // may be empty tables set
    pub(crate) this_range: KeyRange,
    pub(crate) next_range: KeyRange,
    pub(crate) this_size: AtomicU64, // the compacted table's size(NOTE: this level compacted table is only one, not zero level)
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

impl Default for CompactDef {
    fn default() -> Self {
        // CompactDef {
        //     this_level: XWeak::new(),
        //     next_level: XWeak::new(),
        //     top: RwLockReadGuard::,
        //     bot: RwLock::new(vec![]),
        //     this_range: KeyRange {
        //         left: vec![],
        //         right: vec![],
        //         inf: false,
        //     },
        //     next_range: KeyRange {
        //         left: vec![],
        //         right: vec![],
        //         inf: false,
        //     },
        //     this_size: Default::default(),
        // }
        todo!()
    }
}

impl CompactDef {
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
    fn lock_exclusive_levels(&self) {
        self.this_level.lock_exclusive();
        self.next_level.lock_exclusive();
    }

    #[inline]
    fn unlock_exclusive_levels(&self) {
        self.next_level.unlock_exclusive();
        self.this_level.unlock_exclusive();
    }
}
