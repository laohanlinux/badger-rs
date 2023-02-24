use crate::levels::{LevelsController, XLevelsController};
use crate::manifest::{open_or_create_manifest_file, Manifest, ManifestFile};
use crate::options::Options;
use crate::table::builder::Builder;
use crate::table::iterator::IteratorImpl;
use crate::table::table::{new_file_name, Table, TableCore};
use crate::types::{Channel, Closer, XArc, XWeak};
use crate::value_log::{ArcRequest, Entry, MetaBit, Request, ValueLogCore, ValuePointer};
use crate::y::{
    async_sync_directory, create_synced_file, sync_directory, Encode, Result, ValueStruct,
};
use crate::Error::Unexpected;
use crate::{Decode, Error, Node, SkipList, SkipListManager};
use bytes::BufMut;
use drop_cell::defer;
use fs2::FileExt;
use log::{info, Log};
use parking_lot::Mutex;
use std::borrow::BorrowMut;
use std::fs::{read_dir, File};
use std::fs::{try_exists, OpenOptions};
use std::io::{Cursor, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::fs::create_dir_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{RwLock, RwLockWriteGuard};

const _BADGER_PREFIX: &[u8; 8] = b"!badger!";
// Prefix for internal keys used by badger.
const _HEAD: &[u8; 11] = b"!bager!head"; // For Storing value offset for replay.

struct Closers {
    update_size: Closer,
    compactors: Closer,
    mem_table: Closer,
    writes: Closer,
    value_gc: Closer,
}

struct FlushTask {
    mt: Option<SkipList>,
    vptr: ValuePointer,
}

impl FlushTask {
    fn must_mt(&self) -> &SkipList {
        self.mt.as_ref().unwrap()
    }
}

pub struct KV {
    pub opt: Options,
    pub vlog: Option<ValueLogCore>,
    pub manifest: Arc<RwLock<ManifestFile>>,
    lc: XWeak<LevelsController>,
    flush_chan: Channel<FlushTask>,
    // write_chan: Channel<Request>,
    dir_lock_guard: File,
    value_dir_guard: File,
    closers: Closers,
    // Our latest (actively written) in-memory table.
    mt: Option<Arc<SkipList>>,
    mem_st_manger: Arc<SkipListManager>,
    // Add here only AFTER pushing to flush_ch
    imm: Vec<Arc<SkipList>>,
    write_ch: Channel<ArcRequest>,
    // Incremented in the non-concurrently accessed write loop.  But also accessed outside. So
    // we use an atomic op.
    last_used_cas_counter: AtomicU64,
}

// TODO not add bellow lines
unsafe impl Send for KV {}
unsafe impl Sync for KV {}

impl KV {
    pub async fn Open(mut opt: Options) -> Result<XArc<KV>> {
        opt.max_batch_size = (15 * opt.max_table_size) / 100;
        opt.max_batch_count = opt.max_batch_size / Node::size() as u64;
        create_dir_all(opt.dir.as_str()).await?;
        create_dir_all(opt.value_dir.as_str()).await?;
        if !(opt.value_log_file_size <= 2 << 30 && opt.value_log_file_size >= 1 << 20) {
            return Err(Error::ValueLogSize);
        }
        let manifest_file = open_or_create_manifest_file(opt.dir.as_str()).await?;
        let dir_lock_guard = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Path::new(opt.dir.as_str()).join("dir_lock_guard.lock"))?;
        dir_lock_guard.lock_exclusive()?;
        let value_dir_guard = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(Path::new(opt.value_dir.as_str()).join("value_dir_guard.lock"))?;
        value_dir_guard.lock_exclusive()?;
        let closers = Closers {
            update_size: Closer::new(),
            compactors: Closer::new(),
            mem_table: Closer::new(),
            writes: Closer::new(),
            value_gc: Closer::new(),
        };
        // go out.updateSize(out.closers.updateSize)
        let mut out = KV {
            opt: opt.clone(),
            vlog: None,
            manifest: Arc::new(RwLock::new(manifest_file)),
            // lc: Default::default(),
            lc: XWeak::new(),
            flush_chan: Channel::new(1),
            // write_chan: Channel::new(1),
            dir_lock_guard,
            value_dir_guard,
            closers,
            mt: None,
            imm: Vec::new(),
            write_ch: Channel::new(1),
            last_used_cas_counter: Default::default(),
            mem_st_manger: Arc::new(SkipListManager::default()),
        };

        let manifest = out.manifest.clone();

        // handle levels_controller
        let lc = LevelsController::new(manifest.clone(), out.opt.clone()).await?;
        lc.start_compact(out.closers.compactors.clone());

        let mut vlog = ValueLogCore::default();
        vlog.open(&out, opt)?;
        out.vlog = Some(vlog);

        let xout = XArc::new(out);
        // update size
        {
            let _out = xout.clone();
            tokio::spawn(async move {
                _out.spawn_update_size().await;
            });
        }
        // mem_table closer
        {
            let _out = xout.clone();
            tokio::spawn(async move {
                _out.flush_mem_table(_out.closers.mem_table.clone())
                    .await
                    .expect("TODO: panic message");
            });
        }

        let item = xout.get(_HEAD);
        if item.is_err() {
            return Err("Retrieving head".into());
        }
        let item = item.unwrap();

        let value = &item.value;
        if value != _HEAD {
            return Err("Retrieving head".into());
        }

        // lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
        // written value log entry that we replay.  (Subsequent value log entries might be _less_
        // than lastUsedCasCounter, if there was value log gc so we have to max() values while
        // replaying.)
        xout.last_used_cas_counter
            .store(item.cas_counter, Ordering::Relaxed);

        let mut vptr = ValuePointer::default();
        vptr.dec(&mut Cursor::new(value))?;

        let replay_closer = Closer::new();
        {
            let _out = xout.clone();
            let replay_closer = replay_closer.clone();
            tokio::spawn(async move {
                _out.do_writes(replay_closer).await;
            });
        }

        let mut first = true;
        xout.vlog.as_ref().unwrap().replay(&vptr, |entry, vptr| {
            if first {
                info!("First key={}", String::from_utf8_lossy(&entry.key));
            }
            first = false;
            if xout.last_used_cas_counter.load(Ordering::Relaxed) < entry.cas_counter {
                xout.last_used_cas_counter
                    .store(entry.cas_counter, Ordering::Relaxed);
            }
            if entry.cas_counter_check != 0 {
                let old_value = xout.get(&entry.key)?;
                if old_value.cas_counter != entry.cas_counter_check {
                    return Ok(true);
                }
            }
            let mut nv = vec![];
            let mut meta = entry.meta;
            if xout.should_write_value_to_lsm(entry) {
                nv = entry.value.clone();
            } else {
                nv = Vec::with_capacity(ValuePointer::value_pointer_encoded_size());
                vptr.enc(&mut nv).unwrap();
                meta = meta | MetaBit::BIT_VALUE_POINTER.bits();
            }
            let v = ValueStruct {
                meta,
                user_meta: entry.user_meta,
                cas_counter: entry.cas_counter,
                value: nv,
            };
            while let Err(err) = xout.ensure_room_for_write() {}
            todo!()
        })?;
        // Wait for replay to be applied first.
        replay_closer.signal_and_wait().await;
        Ok(xout)
    }

    /// close kv, should be call only once
    pub async fn close(&self) -> Result<()> {
        self.dir_lock_guard
            .unlock()
            .map_err(|err| Unexpected(err.to_string()))?;
        self.value_dir_guard
            .unlock()
            .map_err(|err| Unexpected(err.to_string()))?;
        self.closers.compactors.signal_and_wait().await;
        self.closers.mem_table.signal_and_wait().await;
        self.closers.writes.signal_and_wait().await;
        self.closers.update_size.signal_and_wait().await;
        Ok(())
    }
}

impl KV {
    async fn walk_dir(dir: &str) -> Result<(u64, u64)> {
        let mut lsm_size = 0;
        let mut vlog_size = 0;
        let mut entries = tokio::fs::read_dir("dir").await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            if meta.is_dir() {
                continue;
            }
            if entry.file_name().to_str().unwrap().ends_with(".sst") {
                lsm_size += meta.len();
            } else if entry.file_name().to_str().unwrap().ends_with(".vlog") {
                vlog_size += meta.len();
            }
        }
        Ok((lsm_size, vlog_size))
    }

    // get returns the value in `mem_table` or disk for given key.
    // Note that value will include meta byte.
    pub(crate) fn get(&self, key: &[u8]) -> Result<ValueStruct> {
        let tables = self.get_mem_tables();

        // TODO add metrics
        for tb in tables {
            let vs = tb.get(key);
            if vs.is_none() {
                continue;
            }
            let vs = vs.unwrap();
            // TODO why
            if vs.meta != 0 && !vs.value.is_empty() {
                return Ok(vs);
            }
        }

        todo!()
    }

    // Returns the current `mem_tables` and get references.
    fn get_mem_tables(&self) -> Vec<Arc<SkipList>> {
        // TODO add kv lock
        let mut tables = Vec::with_capacity(self.imm.len() + 1);
        // Get mutable `mem_tables`.
        tables.push(self.mt.as_ref().unwrap().clone());
        tables[0].incr_ref();

        // Get immutable `mem_tables`.
        for tb in self.imm.iter().rev() {
            tb.incr_ref();
            tables.push(tb.clone());
        }
        tables
    }

    // Called serially by only on goroutine
    async fn write_requests(&self, reqs: Arc<Vec<ArcRequest>>) -> Result<()> {
        if reqs.is_empty() {
            return Ok(());
        }
        info!("write_requests called. Writing to value log");
        // CAS counter for all operations has to go onto value log. Otherwise, if it is just in
        // memtable for a long time, and following CAS operations use that as a check, when
        // replaying, we will think that these CAS operations should fail, when they are actually
        // valid.

        // There is code (in flush_mem_table) whose correctness depends on us generating CAS Counter
        // values _before_ we modify s.vptr here.
        for req in reqs.iter() {
            let counter_base = self.new_cas_counter(req.get_req().entries.read().len() as u64);
            for (idx, entry) in req.get_req().entries.read().iter().enumerate() {
                entry.borrow_mut().cas_counter = counter_base + idx as u64;
            }
        }

        self.vlog.as_ref().unwrap().write(reqs.clone())?;
        info!("Writing to memory table");
        let mut count = 0;
        for req in reqs.iter() {
            if req.get_req().entries.read().is_empty() {
                continue;
            }
            count += req.get_req().entries.read().len();
        }
        Ok(())
    }

    // async to flush memory table into zero level
    async fn flush_mem_table(&self, lc: Closer) -> Result<()> {
        while let Ok(task) = self.flush_chan.recv().await {
            if task.mt.is_none() {
                break;
            }
            // TODO if is zero?
            if !task.vptr.is_zero() {
                info!("Storing offset: {:?}", task.vptr);
                let mut offset = vec![0u8; ValuePointer::value_pointer_encoded_size()];
                task.vptr.enc(&mut offset).unwrap();
                // CAS counter is needed and is desirable -- it's the first value log entry
                // we replay, so to speak, perhaps the only, and we use it to re-initialize
                // the CAS counter.
                //
                // The write loop generates CAS counter values _before_ it sets vptr.  It
                // is crucial that we read the cas counter here _after_ reading vptr.  That
                // way, our value here is guaranteed to be >= the CASCounter values written
                // before vptr (because they don't get replayed).
                let value = ValueStruct {
                    meta: 0,
                    user_meta: 0,
                    cas_counter: self.last_used_cas_counter.load(Ordering::Acquire),
                    value: offset,
                };
                task.must_mt().put(_HEAD, value);
            }
            let fid = self.must_lc().reserve_file_id();
            let f_name = new_file_name(fid, &self.opt.dir);
            let fp = create_synced_file(&f_name, true)?;
            // Don't block just to sync the directory entry.
            // TODO use currency
            async_sync_directory(self.opt.dir.clone().to_string()).await?;
            let mut fp = tokio::fs::File::from_std(fp);
            write_level0_table(&task.mt.as_ref().unwrap(), &mut fp).await?;

            let fp = fp.into_std().await;

            let tc = TableCore::open_table(fp, &f_name, self.opt.table_loading_mode)?;
            let tb = Table::new(tc);
            // We own a ref on tbl.
            // self.must_lc().add_level0_table(tb.clone()).await?; // This will incr_ref (if we don't error, sure)
            tb.decr_ref(); // releases our ref.

            // Update s.imm, need a lock.
            // assert!(task.mt.as_ref().unwrap(), "{}", self.imm[0]);
            // TODO
            task.mt.as_ref().unwrap().decr_ref(); // Return memory
        }
        Ok(())
    }

    fn new_cas_counter(&self, how_many: u64) -> u64 {
        self.last_used_cas_counter
            .fetch_add(how_many, Ordering::Relaxed)
    }
}

impl KV {
    fn must_lc(&self) -> XArc<LevelsController> {
        let lc = self.lc.upgrade().unwrap();
        lc
    }

    fn must_mt(&self) -> &Arc<SkipList> {
        self.mt.as_ref().unwrap()
    }
}

pub type WeakKV = XWeak<KV>;

pub type ArcKV = XArc<KV>;

impl ArcKV {
    /// data size stats
    /// TODO
    pub async fn spawn_update_size(&self) {
        let lc = self.closers.update_size.spawn();
        defer! {
            lc.done();
            info!("exit update size worker");
        }

        let mut tk = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
        let dir = self.opt.dir.clone();
        let vdir = self.opt.value_dir.clone();
        loop {
            let c = lc.has_been_closed();
            tokio::select! {
                _ = tk.tick() => {
                    info!("ready to update size");
                    // If value directory is different from dir, we'd have to do another walk.
                    let (lsm_sz, vlog_sz) = KV::walk_dir(dir.as_str()).await.unwrap();
                    if dir != vdir {
                         let (_, vlog_sz) = KV::walk_dir(dir.as_str()).await.unwrap();
                    }
                },
                _ = c.recv() => {

                },
            }
        }
    }

    pub async fn manifest_wl(&self) -> RwLockWriteGuard<'_, ManifestFile> {
        self.manifest.write().await
    }

    pub async fn close(&self) -> Result<()> {
        info!("Closing database");
        // Stop value GC first;
        self.to_ref().closers.value_gc.signal_and_wait().await;
        // Stop writes next.
        self.to_ref().closers.writes.signal_and_wait().await;

        Ok(())
    }
}

impl ArcKV {
    async fn do_writes(&self, lc: Closer) {
        // TODO add metrics
        let has_been_close = lc.has_been_closed();
        let write_ch = self.write_ch.clone();
        let reqs = Arc::new(Mutex::new(vec![]));
        loop {
            tokio::select! {
                _ = has_been_close.recv() => {
                    break;
                },
                req = write_ch.recv() => {
                    reqs.lock().push(req.unwrap());
                }
            }
            // TODO avoid memory allocate again
            if reqs.lock().len() == 100 {
                let to_reqs = reqs
                    .lock()
                    .clone()
                    .into_iter()
                    .map(|req| req.clone())
                    .collect::<Vec<_>>();
                let to_reqs = Arc::new(to_reqs);
                if let Err(err) = self.write_requests(to_reqs).await {
                    let ret = Arc::new(Err(err));
                    reqs.lock().iter().for_each(|req| req.set_err(ret.clone()));
                }
                reqs.lock().clear();
            }
        }

        // clear future requests
        write_ch.close();
        loop {
            let req = write_ch.try_recv();
            if req.is_err() {
                break;
            }
            let req = req.unwrap();
            reqs.lock().push(req);
            let to_reqs = reqs
                .lock()
                .clone()
                .into_iter()
                .map(|req| req.clone())
                .collect::<Vec<_>>();
            if let Err(err) = self.write_requests(Arc::new(to_reqs)).await {
                let ret = Arc::new(Err(err));
                reqs.lock().iter().for_each(|req| req.set_err(ret.clone()));
            }
        }
    }

    fn should_write_value_to_lsm(&self, entry: &Entry) -> bool {
        entry.value.len() < self.opt.value_threshold
    }

    // Always called serially.
    fn ensure_room_for_write(&self) -> Result<()> {
        // TODO a special global lock for this function
        if self.must_mt().mem_size() < self.opt.max_table_size as u32 {
            return Ok(());
        }

        // A nil mt indicates that KV is being closed.
        assert!(!self.must_mt().empty());
        // let flush_task = FlushTask { mt: Some(self.mt), vptr: self.vlog.as_ref().unwrap() }
        todo!()
    }
}

async fn write_level0_table(st: &SkipList, f: &mut tokio::fs::File) -> Result<()> {
    let cur = st.new_cursor();
    let mut builder = Builder::default();
    while let Some(_) = cur.next() {
        let key = cur.key();
        let value = cur.value();
        builder.add(key, &value)?;
    }
    f.write_all(&builder.finish()).await?;
    Ok(())
}

fn arena_size(opt: &Options) -> usize {
    (opt.max_table_size + opt.max_batch_size + opt.max_batch_count * Node::size() as u64) as usize
}
