use crate::iterator::{IteratorExt, IteratorOptions, KVItemInner};
use crate::levels::{LevelsController, XLevelsController};
use crate::manifest::{open_or_create_manifest_file, Manifest, ManifestFile};
use crate::options::Options;
use crate::table::builder::Builder;
use crate::table::iterator::{IteratorImpl, IteratorItem};
use crate::table::table::{new_file_name, Table, TableCore};
use crate::types::{ArcMx, Channel, Closer, TArcMx, XArc, XWeak};
use crate::value_log::{
    ArcRequest, Entry, MetaBit, Request, ValueLogCore, ValuePointer, MAX_KEY_SIZE,
};
use crate::y::{
    async_sync_directory, create_synced_file, sync_directory, Encode, Result, ValueStruct,
};
use crate::Error::{NotFound, Unexpected};
use crate::{
    Decode, Error, MergeIterOverBuilder, MergeIterOverIterator, Node, SkipList, SkipListManager,
    UniIterator, Xiterator,
};
use atomic::Atomic;
use bytes::BufMut;
use crossbeam_epoch::Shared;
use drop_cell::defer;
use fs2::FileExt;
use log::{info, Log};
use parking_lot::Mutex;
use std::cell::RefCell;
use std::fs::File;
use std::fs::{try_exists, OpenOptions};
use std::future::Future;
use std::io::{Cursor, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{string, vec};
use tokio::fs::create_dir_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{RwLock, RwLockWriteGuard};

pub const _BADGER_PREFIX: &[u8; 8] = b"!badger!";
// Prefix for internal keys used by badger.
pub const _HEAD: &[u8; 11] = b"!bager!head"; // For Storing value offset for replay.

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
    pub vptr: crossbeam_epoch::Atomic<ValuePointer>,
    pub manifest: Arc<RwLock<ManifestFile>>,
    lc: Option<LevelsController>,
    flush_chan: Channel<FlushTask>,
    // write_chan: Channel<Request>,
    dir_lock_guard: File,
    value_dir_guard: File,
    closers: Closers,
    // Our latest (actively written) in-memory table.
    mem_st_manger: Arc<SkipListManager>,
    // Add here only AFTER pushing to flush_ch
    write_ch: Channel<ArcRequest>,
    // Incremented in the non-concurrently accessed write loop.  But also accessed outside. So
    // we use an atomic op.
    last_used_cas_counter: AtomicU64,
    share_lock: tokio::sync::RwLock<()>,
    // TODO user ctx replace closer
    ctx: tokio_context::context::Context,
    ctx_handle: tokio_context::context::Handle,
}

unsafe impl Send for KV {}
unsafe impl Sync for KV {}

pub struct BoxKV {
    pub kv: *const KV,
}

unsafe impl Send for BoxKV {}

unsafe impl Sync for BoxKV {}

impl BoxKV {
    pub(crate) fn new(kv: *const KV) -> BoxKV {
        BoxKV { kv }
    }
}

impl KV {
    pub async fn open(mut opt: Options) -> Result<XArc<KV>> {
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
            update_size: Closer::new("update_size".to_owned()),
            compactors: Closer::new("compactors".to_owned()),
            mem_table: Closer::new("mem_table".to_owned()),
            writes: Closer::new("writes".to_owned()),
            value_gc: Closer::new("value_gc".to_owned()),
        };

        let (ctx, h) = tokio_context::context::Context::new();
        let mut out = KV {
            opt: opt.clone(),
            vlog: None,
            vptr: crossbeam_epoch::Atomic::null(),
            manifest: Arc::new(RwLock::new(manifest_file)),
            lc: None,
            flush_chan: Channel::new(1),
            dir_lock_guard,
            value_dir_guard,
            closers,
            write_ch: Channel::new(1),
            last_used_cas_counter: Default::default(),
            mem_st_manger: Arc::new(SkipListManager::new(opt.arena_size() as usize)),
            share_lock: tokio::sync::RwLock::new(()),
            ctx,
            ctx_handle: h,
        };

        let manifest = out.manifest.clone();

        // handle levels_controller
        let lc = LevelsController::new(manifest.clone(), out.opt.clone()).await?;
        lc.start_compact(out.closers.compactors.clone());
        out.lc.replace(lc);
        let mut vlog = ValueLogCore::default();
        {
            let kv = unsafe { &out as *const KV };
            vlog.open(kv, opt)?;
        }
        out.vlog.replace(vlog);

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
                _out.flush_mem_table(_out.closers.mem_table.spawn())
                    .await
                    .expect("TODO: panic message");
            });
        }

        // Get the lasted ValueLog Recover Pointer
        let item = match xout.get(_HEAD) {
            Err(NotFound) => ValueStruct::default(), // Give it a default value
            Err(_) => return Err("Retrieving head".into()),
            Ok(item) => item,
        };
        let value = &item.value;
        assert!(item.value.is_empty() || item.value == _HEAD.to_vec());
        // lastUsedCasCounter will either be the value stored in !badger!head, or some subsequently
        // written value log entry that we replay.  (Subsequent value log entries might be _less_
        // than lastUsedCasCounter, if there was value log gc so we have to max() values while
        // replaying.)
        xout.last_used_cas_counter
            .store(item.cas_counter, Ordering::Relaxed);

        let mut vptr = ValuePointer::default();
        if !item.value.is_empty() {
            vptr.dec(&mut Cursor::new(value))?;
        }
        let replay_closer = Closer::new("tmp_replay".to_owned());
        {
            let _out = xout.clone();
            let replay_closer = replay_closer.spawn();
            tokio::spawn(async move {
                _out.do_writes(replay_closer).await;
            });
        }

        let mut first = true;
        xout.vlog
            .as_ref()
            .unwrap()
            .replay(&vptr, |entry, vptr| {
                let xout = xout.clone();
                Box::pin(async move {
                    if first {
                        info!("First key={}", string::String::from_utf8_lossy(&entry.key));
                    }
                    first = false;
                    if xout.last_used_cas_counter.load(Ordering::Relaxed) < entry.cas_counter {
                        xout.last_used_cas_counter
                            .store(entry.cas_counter, Ordering::Relaxed);
                    }

                    // TODO why?
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
                    while let Err(err) = xout.ensure_room_for_write().await {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    xout.must_mt().put(&entry.key, v);
                    Ok(true)
                })
            })
            .await?;
        // Wait for replay to be applied first.
        replay_closer.signal_and_wait().await;
        // Mmap writeable log
        // let max_fid = xout.must_vlog().max_fid.load(Ordering::Relaxed);
        // let lf = xout.must_vlog().pick_log_by_vlog_id(&max_fid);
        // TODO

        {
            let closer = xout.closers.writes.spawn();
            let _out = xout.clone();
            tokio::spawn(async move {
                _out.do_writes(closer).await;
            });
        }

        {
            let closer = xout.closers.value_gc.spawn();
            let _out = xout.clone();
            tokio::spawn(async move {
                _out.must_vlog().wait_on_gc(closer).await;
            });
        }

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
        let p = crossbeam_epoch::pin();
        let tables = self.get_mem_tables(&p);

        // TODO add metrics
        for tb in tables {
            let vs = unsafe { tb.as_ref().unwrap().get(key) };
            if vs.is_none() {
                continue;
            }
            let vs = vs.unwrap();
            // TODO why
            if vs.meta != 0 && !vs.value.is_empty() {
                return Ok(vs);
            }
        }

        self.must_lc().get(key).ok_or(NotFound)
    }

    // Returns the current `mem_tables` and get references.
    fn get_mem_tables<'a>(&'a self, p: &'a crossbeam_epoch::Guard) -> Vec<Shared<'a, SkipList>> {
        self.mem_st_manger.lock_exclusive();
        defer! {self.mem_st_manger.unlock_exclusive()}

        let mt = self.mem_st_manger.mt_ref(p);
        let mut tables = Vec::with_capacity(self.mem_st_manger.imm().len() + 1);
        // Get mutable `mem_tables`.
        tables.push(mt);
        // TODO
        unsafe { tables[0].as_ref().unwrap().incr_ref() };
        // Get immutable `mem_tables`.
        for tb in self.mem_st_manger.imm().iter().rev() {
            let tb = tb.load(Ordering::Relaxed, &p);
            unsafe { tb.as_ref().unwrap().incr_ref() };
            tables.push(tb);
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
            let entries = req.req_ref().entries.write();
            let counter_base = self.new_cas_counter(entries.len() as u64);
            for (idx, entry) in entries.iter().enumerate() {
                entry.borrow_mut().cas_counter = counter_base + idx as u64;
            }
        }

        self.vlog.as_ref().unwrap().write(reqs.clone())?;
        info!("Writing to memory table");
        let mut count = 0;
        for req in reqs.iter() {
            count += req.get_req().entries.read().len();
        }
        Ok(())
    }

    // async to flush memory table into zero level
    async fn flush_mem_table(&self, lc: Closer) -> Result<()> {
        defer! {lc.done()}
        defer! {info!("exit flush table worker")};
        while let Ok(task) = self.flush_chan.recv().await {
            // after kv send empty mt, it will close flush_chan, so we should return the job.
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
            let task1 = async_sync_directory(self.opt.dir.clone().to_string());
            let mut fp = tokio::fs::File::from_std(fp);
            let task2 = write_level0_table(&task.mt.as_ref().unwrap(), &mut fp);
            let (task1_res, task2_res) = tokio::join!(task1, task2);
            task1_res?;
            task2_res?;

            let fp = fp.into_std().await;
            let tc = TableCore::open_table(fp, &f_name, self.opt.table_loading_mode)?;
            let tb = Table::from(tc);
            // We own a ref on tbl.
            self.must_lc().add_level0_table(tb.clone()).await?;
            // This will incr_ref (if we don't error, sure)
            tb.decr_ref(); // releases our ref.
            self.mem_st_manger.advance_imm(task.must_mt()); // Update s.imm, need a lock.
            task.must_mt().decr_ref(); // Return memory
        }
        Ok(())
    }

    // Applies a list of `badger.entries`. If a request level error occurs it will be returned. Errors are also set on each
    // `Entry` and must be checked individually.
    // Check(kv.batch_set(entries))
    // for e in entries {
    //      Check(e.Error);
    // }
    // TODO
    pub(crate) async fn batch_set(&self, entries: Vec<Entry>) -> Result<Vec<ArcRequest>> {
        let mut bad = vec![];
        let mut b = vec![Request::default()];
        let mut count = 0;
        let mut sz = 0u64;
        for entry in entries {
            if entry.key.len() > MAX_KEY_SIZE {
                bad.push(entry);
                continue;
            }
            if entry.value.len() as u64 > self.opt.value_log_file_size {
                bad.push(entry);
                continue;
            }
            count += 1;
            sz += self.opt.estimate_size(&entry) as u64;
            let req = b.last_mut().unwrap();
            req.entries.write().push(RefCell::new(entry));
            if count >= self.opt.max_batch_count || sz >= self.opt.max_batch_count {
                b.push(Request::default());
            }
        }
        let mut reqs = vec![];
        for req in b {
            if req.entries.read().is_empty() {
                break;
            }
            let arc_req = ArcRequest::from(req);
            reqs.push(arc_req.clone());
            self.write_ch.send(arc_req).await.unwrap();
        }
        if !bad.is_empty() {
            let req = Request::default();
            *req.entries.write() =
                Vec::from_iter(bad.into_iter().map(|bad| RefCell::new(bad)).into_iter());
            let arc_req = ArcRequest::from(req);
            arc_req
                .set_err(Err("key too big or value to big".into()))
                .await;
            reqs.push(arc_req);
        }
        Ok(reqs)
    }

    fn new_cas_counter(&self, how_many: u64) -> u64 {
        self.last_used_cas_counter
            .fetch_add(how_many, Ordering::Relaxed)
    }
}

impl KV {
    pub(crate) fn must_lc(&self) -> &LevelsController {
        let lc = self.lc.as_ref().unwrap();
        lc
    }

    fn must_mt(&self) -> &SkipList {
        let p = crossbeam_epoch::pin();
        let st = self.mem_st_manger.mt_ref(&p).as_raw();
        unsafe { &*st }
    }

    fn must_vlog(&self) -> &ValueLogCore {
        self.vlog.as_ref().unwrap()
    }

    fn must_vptr(&self) -> ValuePointer {
        let p = crossbeam_epoch::pin();
        let ptr = self.vptr.load(Ordering::Relaxed, &p);
        if ptr.is_null() {
            return ValuePointer::default();
        }
        unsafe { ptr.as_ref().unwrap().clone() }
    }
}

pub type WeakKV = XWeak<KV>;

pub type ArcKV = XArc<KV>;

impl ArcKV {
    /// Async open a KV db
    pub async fn open(op: Options) -> Result<ArcKV> {
        KV::open(op).await
    }

    /// data size stats
    /// TODO
    pub async fn spawn_update_size(&self) {
        let lc = self.closers.update_size.spawn();
        defer! {lc.done()}
        defer! {info!("exit update size worker");}

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
                _ = c.recv() => {return;},
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

        self.flush_chan
            .send(FlushTask {
                mt: None,
                vptr: ValuePointer::default(),
            })
            .await
            .unwrap();
        Ok(())
    }

    // NewIterator returns a new iterator. Depending upon the options, either only keys, or both
    // key-value pairs would be fetched. The keys are returned in lexicographically sorted order.
    // Usage:
    //   opt := badger.DefaultIteratorOptions
    //   itr := kv.NewIterator(opt)
    //   for itr.Rewind(); itr.Valid(); itr.Next() {
    //     item := itr.Item()
    //     key := item.Key()
    //     var val []byte
    //     err = item.Value(func(v []byte) {
    //         val = make([]byte, len(v))
    // 	       copy(val, v)
    //     }) 	// This could block while value is fetched from value log.
    //          // For key only iteration, set opt.PrefetchValues to false, and don't call
    //          // item.Value(func(v []byte)).
    //
    //     // Remember that both key, val would become invalid in the next iteration of the loop.
    //     // So, if you need access to them outside, copy them or parse them.
    //   }
    //   itr.Close()
    pub(crate) async fn new_iterator(&self, opt: IteratorOptions) -> IteratorExt {
        let p = crossbeam_epoch::pin();
        let tables = self.get_mem_tables(&p);
        defer! {
            tables.iter().for_each(|table| unsafe {table.as_ref().unwrap().decr_ref()});
        }
        // add vlog reference.
        self.must_vlog().incr_iterator_count();

        // Create iterators across all the tables involved first.
        let mut itrs: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
        for tb in tables.clone() {
            let st = unsafe { tb.as_ref().unwrap().clone() };
            let iter = Box::new(UniIterator::new(st, opt.reverse));
            itrs.push(iter);
        }
        itrs.extend(self.must_lc().as_iterator(opt.reverse));
        let mitr = MergeIterOverBuilder::default().add_batch(itrs).build();
        IteratorExt::new(self.clone(), mitr, opt)
    }
}

impl ArcKV {
    async fn do_writes(&self, lc: Closer) {
        info!("start do writes task!");
        defer! {lc.done();};
        defer! {info!("exit do write woker")};
        // TODO add metrics
        let has_been_close = lc.has_been_closed();
        let write_ch = self.write_ch.clone();
        let reqs = ArcMx::<Vec<ArcRequest>>::new(Mutex::new(vec![]));
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
                    // for req in reqs.lock().iter() {
                    //     req.set_err(Err(err.clone())).await;
                    // }
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
                // for req in reqs.lock().iter() {
                //     req.set_err(Err(err.clone())).await;
                // }
            }
        }
    }

    fn should_write_value_to_lsm(&self, entry: &Entry) -> bool {
        entry.value.len() < self.opt.value_threshold
    }

    // Always called serially.
    async fn ensure_room_for_write(&self) -> Result<()> {
        // TODO a special global lock for this function
        let _ = self.share_lock.write().await;
        if self.must_mt().mem_size() < self.opt.max_table_size as u32 {
            return Ok(());
        }

        // A nil mt indicates that KV is being closed.
        assert!(!self.must_mt().empty());

        let flush_task = FlushTask {
            mt: Some(self.must_mt().clone()),
            vptr: self.must_vptr(),
        };
        if let Ok(_) = self.flush_chan.try_send(flush_task) {
            info!("Flushing value log to disk if async mode.");
            // Ensure value log is synced to disk so this memtable's contents wouldn't be lost.
            self.must_vlog().sync()?;
            info!(
                "Flushing memtable, mt.size={} size of flushChan: {}",
                self.must_mt().mem_size(),
                self.flush_chan.tx().len()
            );
            // We manage to push this task. Let's modify imm.
            self.mem_st_manger.swap_st(self.opt.clone());
            // New memtable is empty. We certainly have room.
            Ok(())
        } else {
            Err(Unexpected("No room for write".into()))
        }
    }

    // asyn yield item value from ValueLog
    pub(crate) async fn yield_item_value(
        &self,
        item: KVItemInner,
        mut consumer: impl FnMut(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        // no value
        if !item.has_value() {
            return consumer(vec![]).await;
        }

        // TODO What is this
        if (item.meta() & MetaBit::BIT_VALUE_POINTER.bits()) == 0 {
            return consumer(item.vptr().to_vec()).await;
        }

        let mut vptr = ValuePointer::default();
        vptr.dec(&mut Cursor::new(item.vptr()))?;
        let vlog = self.must_vlog();
        vlog.async_read(&vptr, consumer).await?;
        Ok(())
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

#[test]
fn t_pointer() {
    struct Ext {
        v: Vec<u32>,
        name: String,
    }

    let t = Ext {
        v: vec![],
        name: "Name".to_owned(),
    };

    let p = unsafe { &t as *const Ext };

    let arc_p = Arc::new(t);

    print!("==> {:?}", p);
}
