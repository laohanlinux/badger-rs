use crate::iterator::{IteratorExt, IteratorOptions, KVItem, KVItemInner};
use crate::levels::LevelsController;
use crate::manifest::{open_or_create_manifest_file, ManifestFile};
use crate::options::Options;
use crate::table::builder::Builder;
use crate::table::iterator::IteratorItem;
use crate::table::table::{new_file_name, Table, TableCore};
use crate::types::{ArcMx, Channel, Closer, TArcRW, XArc, XWeak};
use crate::value_log::{
    Entry, EntryType, MetaBit, Request, ValueLogCore, ValuePointer, MAX_KEY_SIZE,
};
use crate::y::{async_sync_directory, create_synced_file, Encode, Result, ValueStruct};
use crate::Error::{NotFound, Unexpected};
use crate::{
    hex_str, Decode, Error, MergeIterOverBuilder, Node, SkipList, SkipListManager, UniIterator,
    Xiterator,
};

use atomic::Atomic;
use bytes::BufMut;
use crossbeam_epoch::{Owned, Shared};
use drop_cell::defer;
use fs2::FileExt;

use log::{debug, error, info, trace, warn};

use parking_lot::Mutex;

use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::{Cursor, Write};

use std::path::Path;
use std::pin::Pin;

use rand::random;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{string, vec};
use tokio::fs::create_dir_all;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, RwLockWriteGuard};

pub const _BADGER_PREFIX: &[u8; 8] = b"!badger!";
// Prefix for internal keys used by badger.
pub const _HEAD: &[u8; 11] = b"!bager!head"; // For Storing value offset for replay.

pub struct Closers {
    pub update_size: Closer,
    pub compactors: Closer,
    pub mem_table: Closer,
    // Wait flush job exit
    pub writes: Closer,
    pub value_gc: Closer,
}

#[derive(Debug)]
struct FlushTask {
    mt: Option<SkipList>,
    vptr: ValuePointer,
}

impl FlushTask {
    fn must_mt(&self) -> &SkipList {
        self.mt.as_ref().unwrap()
    }
}

pub struct KVBuilder {
    opt: Options,
    kv: BoxKV,
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
    pub closers: Closers,
    // Our latest (actively written) in-memory table.
    pub mem_st_manger: Arc<SkipListManager>,
    // Add here only AFTER pushing to flush_ch
    pub write_ch: Channel<Request>,
    // Incremented in the non-concurrently accessed write loop.  But also accessed outside. So
    // we use an atomic op.
    pub(crate) last_used_cas_counter: AtomicU64,
    share_lock: tokio::sync::RwLock<()>,
    // TODO user ctx replace closer
    ctx: tokio_context::context::Context,
    ctx_handle: tokio_context::context::Handle,
}

unsafe impl Send for KV {}

unsafe impl Sync for KV {}

impl Drop for KV {
    fn drop(&mut self) {
        info!("Drop kv");
    }
}

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
        info!(">>>>");
        let manifest_file = open_or_create_manifest_file(opt.dir.as_str()).await?;
        info!(">>>>");

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
            last_used_cas_counter: AtomicU64::new(1),
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
            let kv = &out as *const KV;
            vlog.open(kv, opt.clone()).await?;
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
                if let Err(err) = _out.flush_mem_table(_out.closers.mem_table.spawn()).await {
                    error!("abort exit flush mem table {:?}", err);
                } else {
                    info!("abort exit flush mem table");
                }
            });
        }

        // Get the lasted ValueLog Recover Pointer
        let item = match xout._get(_HEAD) {
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
        xout.update_last_used_cas_counter(item.cas_counter);

        let mut vptr = ValuePointer::default();
        if !item.value.is_empty() {
            vptr.dec(&mut Cursor::new(value))?;
        }
        let replay_closer = Closer::new("tmp_writer_closer".to_owned());
        {
            let _out = xout.clone();
            let replay_closer = replay_closer.spawn();
            tokio::spawn(async move {
                _out.do_writes(replay_closer, true).await;
            });
        }

        // replay data from vlog
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
                    // TODO maybe use comparse set
                    if xout.get_last_used_cas_counter() < entry.get_cas_counter() {
                        xout.update_last_used_cas_counter(entry.get_cas_counter());
                    }

                    // TODO why?
                    if entry.cas_counter_check != 0 {
                        let old_value = xout._get(&entry.key)?;
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
                        cas_counter: entry.get_cas_counter(),
                        value: nv,
                    };
                    while let Err(_err) = xout.ensure_room_for_write().await {
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
        let max_fid = xout.must_vlog().max_fid.load(Ordering::Relaxed);
        let lf = xout.must_vlog().pick_log_by_vlog_id(&max_fid).await;
        lf.write()
            .await
            .set_write(opt.clone().value_log_file_size * 2)?;
        // TODO

        {
            let closer = xout.closers.writes.spawn();
            let _out = xout.clone();
            tokio::spawn(async move {
                _out.do_writes(closer, false).await;
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
}

impl KV {
    async fn walk_dir(dir: &str) -> Result<(u64, u64)> {
        let mut lsm_size = 0;
        let mut vlog_size = 0;
        let mut entries = tokio::fs::read_dir(dir).await?;
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
    pub(crate) fn _get(&self, key: &[u8]) -> Result<ValueStruct> {
        let p = crossbeam_epoch::pin();
        let tables = self.get_mem_tables(&p);
        // TODO add metrics
        for tb in tables {
            let st = unsafe { tb.as_ref().unwrap() };
            let vs = st.get(key);
            if vs.is_none() {
                continue;
            }
            let vs = vs.unwrap();
            // TODO why
            if vs.meta != 0 || !vs.value.is_empty() {
                debug!(
                    "fount from skiplist, st:{}, key #{}, value: {}",
                    st.id(),
                    crate::y::hex_str(key),
                    crate::y::hex_str(&vs.value),
                );
                return Ok(vs);
            }
        }
        warn!("found from disk table, key #{}", crate::y::hex_str(key));
        self.must_lc().get(key).ok_or(NotFound)
    }

    // Sets the provided value for a given key. If key is not present, it is created.  If it is
    // present, the existing value is overwritten with the one provided.
    // Along with key and value, Set can also take an optional userMeta byte. This byte is stored
    // alongside the key, and can be used as an aid to interpret the value or store other contextual
    // bits corresponding to the key-value pair.
    pub(crate) async fn set(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) -> Result<()> {
        let res = self
            .batch_set(vec![Entry::default()
                .key(key)
                .value(value)
                .user_meta(user_meta)])
            .await;
        assert_eq!(res.len(), 1);
        res[0].to_owned()
    }

    // Called serially by only on goroutine
    async fn write_requests(&self, reqs: Vec<Request>) -> Result<()> {
        if reqs.is_empty() {
            return Ok(());
        }
        info!(
            "write_requests called. Writing to value log, count: {}",
            reqs.len()
        );

        // CAS counter for all operations has to go onto value log. Otherwise, if it is just in
        // memtable for a long time, and following CAS operations use that as a check, when
        // replaying, we will think that these CAS operations should fail, when they are actually
        // valid.

        // There is code (in flush_mem_table) whose correctness depends on us generating CAS Counter
        // values _before_ we modify s.vptr here.
        for req in reqs.iter() {
            let entries = &req.entries;
            let counter_base = self.new_cas_counter(entries.len() as u64);
            for (idx, entry) in entries.iter().enumerate() {
                entry
                    .entry()
                    .cas_counter
                    .store(counter_base + idx as u64, Ordering::Release);
            }
        }

        if let Err(err) = self.vlog.as_ref().unwrap().write(reqs.clone()).await {
            for req in reqs.iter() {
                req.set_entries_resp(Err(err.clone())).await;
            }
            return Err(err);
        }

        info!("Writing to memory table");
        let mut count = 0;
        for mut req in reqs.into_iter() {
            if req.entries.is_empty() {
                continue;
            }
            count += req.entries.len();
            while let Err(err) = self.ensure_room_for_write().await {
                debug!("failed to ensure room for write!, err:{}", err);
                tokio::time::sleep(Duration::from_millis(10)).await;

                #[cfg(test)]
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            info!("Waiting for write lsm, count {}", count);
            self.update_offset(&mut req.ptrs).await;
            // It should not fail
            self.write_to_lsm(req).await.unwrap();
        }
        info!("{} entries written", count);
        Ok(())
    }

    // async to flush memory table into zero level
    async fn flush_mem_table(&self, lc: Closer) -> Result<()> {
        defer! {lc.done()}
        defer! {info!("exit flush mem table")}
        while let Ok(task) = self.flush_chan.recv().await {
            info!("Receive a flush task, {:?} !!!", task);
            // after kv send empty mt, it will close flush_chan, so we should return the job.
            if task.mt.is_none() {
                warn!("receive a exit task!");
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
                    cas_counter: self.get_last_used_cas_counter(),
                    value: offset,
                };
                task.must_mt().put(_HEAD, value);
            }
            let fid = self.must_lc().reserve_file_id();
            let f_name = new_file_name(fid, &self.opt.dir);
            let fp = create_synced_file(&f_name, true)?;
            // Don't block just to sync the directory entry.
            // let task1 = async_sync_directory(self.opt.dir.clone().to_string());
            // let mut fp = tokio::fs::File::from_std(fp);
            // let task2 = write_level0_table(&task.mt.as_ref().unwrap(), &mut fp);
            // let (task1_res, task2_res) = tokio::join!(task1, task2);
            // task1_res?;
            // task2_res?;

            async_sync_directory(self.opt.dir.clone().to_string()).await?;
            let mut fp = tokio::fs::File::from_std(fp);
            write_level0_table(&task.mt.as_ref().unwrap(), &f_name, &mut fp).await?;

            debug!("Ready to advance im");
            let fp = fp.into_std().await;
            let tc = TableCore::open_table(fp, &f_name, self.opt.table_loading_mode)?;
            let tb = Table::from(tc);
            // We own a ref on tbl.
            self.must_lc().add_level0_table(tb.clone()).await?;
            debug!("Ready to advance im");
            let _ = self.share_lock.write().await;
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
    pub(crate) async fn batch_set(&self, entries: Vec<Entry>) -> Vec<Result<()>> {
        let mut count = 0;
        let mut sz = 0u64;
        let mut res = vec![Ok(()); entries.len()];
        let mut req = Request::default();
        let mut req_index = vec![];

        for (i, entry) in entries.into_iter().enumerate() {
            if entry.key.len() > MAX_KEY_SIZE {
                res[i] = Err("Key too big".into());
                continue;
            }
            if entry.value.len() as u64 > self.opt.value_log_file_size {
                res[i] = Err("Value to big".into());
                continue;
            }
            {
                count += 1;
                sz += self.opt.estimate_size(&entry) as u64;
                req.entries.push(EntryType::from(entry));
                req.ptrs.push(Arc::new(Atomic::new(None)));
                req_index.push(i);
            }

            if count >= self.opt.max_batch_count || sz >= self.opt.max_batch_count {
                assert!(!self.write_ch.is_close());
                info!("send tasks to write, entries: {}", req.entries.len());
                let resp_ch = req.get_resp_channel();
                self.write_ch.send(req).await.unwrap();
                {
                    count = 0;
                    sz = 0;
                    for (index, ch) in resp_ch.into_iter().enumerate() {
                        let entry_index = req_index[index];
                        res[entry_index] = ch.recv().await.unwrap();
                    }
                    req = Request::default();
                    req_index.clear();
                }
            }
        }

        if !req.entries.is_empty() {
            let resp_ch = req.get_resp_channel();
            self.write_ch.send(req).await.unwrap();
            {
                for (index, ch) in resp_ch.into_iter().enumerate() {
                    let entry_index = req_index[index];
                    res[entry_index] = ch.recv().await.unwrap();
                }
                req_index.clear();
            }
        }
        res
    }

    async fn write_to_lsm(&self, req: Request) -> Result<()> {
        assert_eq!(req.entries.len(), req.ptrs.len());
        defer! {info!("exit write to lsm")}

        #[cfg(test)]
        let tid = random::<u32>();

        for (i, pair) in req.entries.into_iter().enumerate() {
            let (entry, resp_ch) = pair.to_owned();

            #[cfg(test)]
            let debug_entry = entry.clone();

            let mut old_cas = 0;

            if entry.cas_counter_check != 0 {
                // TODO FIXME if not found the key，maybe push something to resp_ch
                let old_value = self._get(&entry.key);
                if old_value.is_err() {
                    resp_ch.send(Err(old_value.unwrap_err())).await.unwrap();
                    continue;
                }

                let old_value = old_value.unwrap();

                old_cas = old_value.cas_counter;

                // No need to decode existing value. Just need old CAS counter.
                if old_value.cas_counter != entry.cas_counter_check {
                    resp_ch.send(Err(Error::ValueCasMisMatch)).await.unwrap();

                    #[cfg(test)]
                    warn!(
                        "tid:{}, abort cas check, #{}, old_cas:{}, check_cas: {}, old_value:{}, new_val: {}",
                        tid,
                        crate::y::hex_str(&entry.key),
                        old_cas, entry.cas_counter_check,
                        crate::y::hex_str(&old_value.value),
                        crate::y::hex_str(&entry.value));

                    continue;
                }
            }

            if entry.meta == MetaBit::BIT_SET_IF_ABSENT.bits() {
                // Someone else might have written a value, so lets check again if key exists.
                let exits = self._exists(&entry.key)?;
                // Value already exists. don't write.
                if exits {
                    resp_ch.send(Err(Error::ValueKeyExists)).await.unwrap();
                    continue;
                }
            }

            let key;
            let value;
            if self.should_write_value_to_lsm(&entry) {
                let cas = entry.get_cas_counter();
                let (_key, _value) = (entry.key, entry.value);
                key = _key;
                value = ValueStruct::new(_value, entry.meta, entry.user_meta, cas);
                // Will include deletion/tombstone case.
                info!("Lsm ok, the value not at vlog file");
            } else {
                let ptr = req.ptrs.get(i).unwrap().load(Ordering::Relaxed);
                let ptr = ptr.unwrap();
                let mut wt = Cursor::new(vec![0u8; ValuePointer::value_pointer_encoded_size()]);
                ptr.enc(&mut wt).unwrap();
                let cas = entry.get_cas_counter();
                key = entry.key;
                value = ValueStruct::new(
                    wt.into_inner(),
                    entry.meta | MetaBit::BIT_VALUE_POINTER.bits(),
                    entry.user_meta,
                    cas,
                );
            }
            self.must_mt().put(&key, value);

            #[cfg(test)]
            warn!(
                "tid:{}, st:{}, key #{:?}, old_cas:{}, new_cas:{}, check_cas:{}, value #{:?} has inserted into SkipList!!!",
                tid,
                self.must_mt().id(),
                String::from_utf8_lossy(&key),
                old_cas,
                debug_entry.get_cas_counter(),
                debug_entry.cas_counter_check,
                String::from_utf8_lossy(&debug_entry.value),
            );

            resp_ch.send(Ok(())).await.unwrap();
        }

        Ok(())
    }

    async fn ensure_room_for_write(&self) -> Result<()> {
        defer! {info!("exit ensure room for write!")}
        // TODO a special global lock for this function
        let _ = self.share_lock.write().await;
        if self.must_mt().mem_size() < self.opt.max_table_size as u32 {
            return Ok(());
        }
        info!(
            "flush memory table, {}, {}",
            self.must_mt().mem_size(),
            self.opt.max_table_size
        );
        // A nil mt indicates that KV is being closed.
        assert!(!self.must_mt().empty());
        let flush_task = FlushTask {
            mt: Some(self.must_mt().clone()),
            vptr: self.must_vptr(),
        };
        let ret = self.flush_chan.try_send(flush_task);
        if ret.is_err() {
            info!("No room for write, {:?}", ret.unwrap_err());
            return Err(Unexpected("No room for write".into()));
        }

        info!("Flushing value log to disk if async mode.");
        // Ensure value log is synced to disk so this memtable's contents wouldn't be lost.
        self.must_vlog().sync().await?;
        info!(
            "Flushing memtable, mt.size={} size of flushChan: {}",
            self.must_mt().mem_size(),
            self.flush_chan.tx().len()
        );
        // We manage to push this task. Let's modify imm.
        self.mem_st_manger.swap_st(self.opt.clone());
        // New memtable is empty. We certainly have room.
        Ok(())
    }

    async fn update_offset(&self, ptrs: &mut Vec<Arc<Atomic<Option<ValuePointer>>>>) {
        let mut ptr = ValuePointer::default();
        for tmp_ptr in ptrs.iter().rev() {
            let tmp_ptr = tmp_ptr.load(Ordering::Relaxed);
            if tmp_ptr.is_none() || tmp_ptr.unwrap().is_zero() {
                continue;
            }
            ptr = tmp_ptr.unwrap();
            info!("Update offset, value pointer: {:?}", ptr);
            break;
        }

        if ptr.is_zero() {
            return;
        }

        let _ = self.share_lock.write().await;
        self.vptr.store(Owned::new(ptr), Ordering::Release);
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

    fn _exists(&self, key: &[u8]) -> Result<bool> {
        return match self._get(key) {
            Err(err) if err.is_not_found() => Ok(false),
            Err(err) => Err(err),
            Ok(value) => {
                info!("{:?}", value);
                if value.value.is_empty() && value.meta == 0 {
                    return Ok(false);
                }
                Ok((value.meta & MetaBit::BIT_DELETE.bits()) == 0)
            }
        };
    }

    fn new_cas_counter(&self, how_many: u64) -> u64 {
        self.last_used_cas_counter
            .fetch_add(how_many, Ordering::Relaxed)
            + 1
    }

    fn should_write_value_to_lsm(&self, entry: &Entry) -> bool {
        entry.value.len() < self.opt.value_threshold
    }
}

impl KV {
    pub(crate) fn must_lc(&self) -> &LevelsController {
        let lc = self.lc.as_ref().unwrap();
        lc
    }

    pub(crate) fn must_mt(&self) -> &SkipList {
        let p = crossbeam_epoch::pin();
        let st = self.mem_st_manger.mt_ref(&p).as_raw();
        assert!(!st.is_null());
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

    pub(crate) fn get_last_used_cas_counter(&self) -> u64 {
        self.last_used_cas_counter.load(Ordering::Acquire)
    }

    pub(crate) fn update_last_used_cas_counter(&self, cas: u64) {
        self.last_used_cas_counter.store(cas, Ordering::Release);
    }

    pub(crate) fn incr_last_userd_cas_counter(&self, incr: u64) {
        self.last_used_cas_counter
            .fetch_add(incr, Ordering::Release);
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

        let mut tk = tokio::time::interval(tokio::time::Duration::from_secs(5 * 60));
        let dir = self.opt.dir.clone();
        let vdir = self.opt.value_dir.clone();
        loop {
            let c = lc.has_been_closed();
            tokio::select! {
                _ = tk.tick() => {
                    info!("ready to update size");
                    // If value directory is different from dir, we'd have to do another walk.
                    let (_lsm_sz, _vlog_sz) = KV::walk_dir(dir.as_str()).await.unwrap();
                    if dir != vdir {
                         let (_, _vlog_sz) = KV::walk_dir(dir.as_str()).await.unwrap();
                    }
                },
                _ = c.recv() => {return;},
            }
        }
    }

    pub async fn manifest_wl(&self) -> RwLockWriteGuard<'_, ManifestFile> {
        self.manifest.write().await
    }

    /// Return a value that will async load value, if want not return value, should be `exists`
    pub async fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let got = self._get(key)?;
        let inner = KVItemInner::new(key.to_vec(), got, self.clone());
        inner.get_value().await
    }

    pub(crate) async fn get_with_ext(&self, key: &[u8]) -> Result<KVItem> {
        let got = self._get(key)?;
        let inner = KVItemInner::new(key.to_vec(), got, self.clone());
        Ok(TArcRW::new(tokio::sync::RwLock::new(inner)))
    }

    /// Set sets the provided value for a given key. If key is not present, it is created. If it is
    /// present, the existing value is overwritten with the one provided.
    /// Along with key and value, Set can also take an optional userMeta byte. This byte is stored
    /// alongside the key, and can be used as an aid to interpret the value or store other contextual
    /// bits corresponding to the key-value pair.
    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) -> Result<()> {
        self.to_ref().set(key, value, user_meta).await
    }

    /// Sets value of key if key is not present.
    /// If it is present, it returns the key_exists error.
    /// TODO it should be atomic operate
    pub async fn set_if_ab_sent(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) -> Result<()> {
        let exists = self.exists(&key).await?;
        // found the key, return key_exists
        if exists {
            return Err(Error::ValueKeyExists);
        }
        let entry = Entry::default()
            .key(key)
            .value(value)
            .user_meta(user_meta)
            .meta(MetaBit::BIT_SET_IF_ABSENT.bits());
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// Return Ok(true) if key exists, Ok(false) if key not exists, Otherwise Err(err) if happen some error.
    pub async fn exists(&self, key: &[u8]) -> Result<bool> {
        return self._exists(key);
    }

    /// Batch set entries, returns result sets
    pub async fn batch_set(&self, entries: Vec<Entry>) -> Vec<Result<()>> {
        self.to_ref().batch_set(entries).await
    }

    /// CompareAndSetAsync is the asynchronous version of CompareAndSet. It accepts a callback function
    /// which is called when the CompareAndSet completes. Any error encountered during execution is
    /// passed as an argument to the callback function.
    pub async fn compare_and_set(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        cas_counter: u64,
    ) -> Result<()> {
        let entry = Entry::default()
            .key(key)
            .value(value)
            .cas_counter_check(cas_counter);
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// Delete deletes a key.
    /// Exposing this so that user does not have to specify the Entry directly.
    /// For example, BitDelete seems internal to badger.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let entry = Entry::default().key(key.to_vec());
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// CompareAndDelete deletes a key ensuring that it has not been changed since last read.
    /// If existing key has different casCounter, this would not delete the key and return an error.
    pub async fn compare_and_delete(&self, key: Vec<u8>, cas_counter: u64) -> Result<()> {
        let entry = Entry::default().key(key).cas_counter_check(cas_counter);
        let ret = self.batch_set(vec![entry]).await;
        ret[0].to_owned()
    }

    /// RunValueLogGC would trigger a value log garbage collection with no guarantees that a call would
    /// result in a space reclaim. Every run would in the best case rewrite only one log file. So,
    /// repeated calls may be necessary.
    ///
    /// The way it currently works is that it would randomly pick up a value log file, and sample it. If
    /// the sample shows that we can discard at least discardRatio space of that file, it would be
    /// rewritten. Else, an ErrNoRewrite error would be returned indicating that the GC didn't result in
    /// any file rewrite.
    ///
    /// We recommend setting discardRatio to 0.5, thus indicating that a file be rewritten if half the
    /// space can be discarded.  This results in a lifetime value log write amplification of 2 (1 from
    /// original write + 0.5 rewrite + 0.25 + 0.125 + ... = 2). Setting it to higher value would result
    /// in fewer space reclaims, while setting it to a lower value would result in more space reclaims at
    /// the cost of increased activity on the LSM tree. discardRatio must be in the range (0.0, 1.0),
    /// both endpoints excluded, otherwise an ErrInvalidRequest is returned.
    ///
    /// Only one GC is allowed at a time. If another value log GC is running, or KV has been closed, this
    /// would return an ErrRejected.
    ///
    /// Note: Every time GC is run, it would produce a spike of activity on the LSM tree.
    pub async fn run_value_log_gc(&self, discard_ratio: f64) -> Result<()> {
        if discard_ratio >= 1.0 || discard_ratio <= 0.0 {
            return Err(Error::ValueInvalidRequest);
        }
        self.must_vlog().trigger_gc(discard_ratio).await
    }

    /// Closes a KV. It's crucial to call it to ensure all the pending updates
    /// make their way to disk.
    pub async fn close(&self) -> Result<()> {
        info!("Closing database");
        // Stop value GC first;
        self.to_ref().closers.value_gc.signal_and_wait().await;
        // Stop writes next.
        self.to_ref().closers.writes.signal_and_wait().await;

        // Now close the value log.
        self.must_vlog().close().await?;

        // Make sure that block writer is done pushing stuff into memtable!
        // Otherwise, you will have a race condition: we are trying to flush memtables
        // and remove them completely, while the block / memtable writer is still trying
        // to push stuff into the memtable. This will also resolve the value
        // offset problem: as we push into memtable, we update value offsets there.
        if !self.must_mt().empty() {
            info!("Flushing memtable!");
            let _ = self.share_lock.write().await;
            let vptr = unsafe {
                self.vptr
                    .load(Ordering::Relaxed, &crossbeam_epoch::pin())
                    .as_ref()
                    .clone()
                    .unwrap()
                    .clone()
            };
            assert!(!self.mem_st_manger.mt_ref(&crossbeam_epoch::pin()).is_null());
            self.flush_chan
                .send(FlushTask {
                    mt: Some(self.mem_st_manger.mt_clone()),
                    vptr,
                })
                .await
                .unwrap();
            self.mem_st_manger.swap_st(self.opt.clone());
            info!("Pushed to flush chan");
        }

        // Tell flusher to quit.
        self.flush_chan
            .send(FlushTask {
                mt: None,
                vptr: ValuePointer::default(),
            })
            .await
            .unwrap();
        self.closers.mem_table.signal_and_wait().await;
        info!("Memtable flushed!");

        self.closers.compactors.signal_and_wait().await;
        info!("Compaction finished!");

        self.must_lc().close()?;

        info!("Waiting for closer");
        self.closers.update_size.signal_and_wait().await;

        self.dir_lock_guard.unlock()?;
        self.value_dir_guard.unlock()?;

        self.manifest.write().await.close();
        // Fsync directions to ensure that lock file, and any other removed files whose directory
        // we haven't specifically fsynced, are guaranteed to have their directory entry removal
        // persisted to disk.
        async_sync_directory(self.opt.dir.clone().to_string()).await?;
        async_sync_directory(self.opt.value_dir.clone().to_string()).await?;
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
    async fn do_writes(&self, lc: Closer, without_close_write_ch: bool) {
        info!("start do writes task!");
        defer! {info!("exit writes task!")}
        defer! {lc.done()}
        // TODO add metrics
        let has_been_close = lc.has_been_closed();
        let write_ch = self.write_ch.clone();
        let reqs = ArcMx::<Vec<Request>>::new(Mutex::new(vec![]));
        let to_reqs = || {
            let to_reqs = reqs
                .lock()
                .clone()
                .into_iter()
                .map(|req| req.clone())
                .collect::<Vec<_>>();
            reqs.lock().clear();
            to_reqs
        };
        loop {
            tokio::select! {
                _ret = has_been_close.recv() => {
                    break;
                },
                req = write_ch.recv() => {
                    if req.is_err() {
                        assert!(write_ch.is_close());
                        info!("receive a invalid write task, err: {:?}", req.unwrap_err());
                        break;
                    }
                    reqs.lock().push(req.unwrap());
                }
            }

            let to_reqs = if reqs.lock().len() == 100 {
                to_reqs()
            } else {
                if let Ok(req) = write_ch.try_recv() {
                    reqs.lock().push(req);
                    vec![]
                } else {
                    to_reqs()
                }
            };

            if !to_reqs.is_empty() {
                self.write_requests(to_reqs)
                    .await
                    .expect("TODO: panic message");
            }
        }

        // clear future requests
        if !without_close_write_ch {
            assert!(!write_ch.is_close());
            write_ch.close();
        }
        loop {
            let req = write_ch.try_recv();
            if let Err(err) = &req {
                assert!(err.is_closed() || err.is_empty(), "{:?}", err);
                break;
            }
            reqs.lock().push(req.unwrap());
            let to_reqs = to_reqs();
            self.write_requests(to_reqs.clone())
                .await
                .expect("TODO: panic message");
        }
    }

    // asyn yield item value from ValueLog
    pub(crate) async fn yield_item_value(
        &self,
        item: KVItemInner,
        mut consumer: impl FnMut(&[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        info!("ready to yield item value from vlog!");
        // no value
        if !item.has_value() {
            return consumer(&[0u8; 0]).await;
        }

        // TODO What is this
        if (item.meta() & MetaBit::BIT_VALUE_POINTER.bits()) == 0 {
            return consumer(item.vptr()).await;
        }
        let mut vptr = ValuePointer::default();
        vptr.dec(&mut Cursor::new(item.vptr()))?;
        let vlog = self.must_vlog();
        vlog.async_read(&vptr, consumer).await?;
        Ok(())
    }
}

// Write level zero table
pub(crate) async fn write_level0_table(
    st: &SkipList,
    f_name: &String,
    f: &mut tokio::fs::File,
) -> Result<()> {
    defer! {info!("Finish write level zero table")}

    #[cfg(test)]
    {
        let keys = st
            .key_values()
            .iter()
            .map(|(key, _)| hex_str(key))
            .collect::<Vec<_>>()
            .join("#");
        warn!(
            "write skiplist into table, st: {}, fpath:{}, {:?}",
            st.id(),
            f_name,
            keys,
        );
        crate::test_util::push_log(keys.as_bytes(), false);
    }

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
