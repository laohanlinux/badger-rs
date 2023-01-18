use crate::manifest::{open_or_create_manifest_file, Manifest};
use crate::options::Options;
use crate::table::builder::Builder;
use crate::table::iterator::IteratorImpl;
use crate::types::{Channel, Closer, XArc, XWeak};
use crate::value_log::{Request, ValueLogCore, ValuePointer};
use crate::y::{Encode, Result, ValueStruct};
use crate::Error::Unexpected;
use crate::{Error, Node, SkipList};
use fs2::FileExt;
use log::info;
use std::borrow::BorrowMut;
use std::fs::{create_dir_all, read_dir, File};
use std::fs::{try_exists, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

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

pub struct KV {
    pub opt: Options,
    pub vlog: Option<ValueLogCore>,
    pub manifest: Manifest,
    flush_chan: Channel<FlushTask>,
    // write_chan: Channel<Request>,
    dir_lock_guard: File,
    value_dir_guard: File,
    closers: Closers,
    // Our latest (actively written) in-memory table.
    mt: SkipList,
    // Add here only AFTER pushing to flush_ch
    imm: Vec<SkipList>,
    // Incremented in the non-concurrently accessed write loop.  But also accessed outside. So
    // we use an atomic op.
    last_used_cas_counter: AtomicU64,
}

impl KV {
    pub fn new(opt: Options) -> Result<XArc<KV>> {
        let mut _opt = opt.clone();
        _opt.max_batch_size = (15 * opt.max_table_size) / 100;
        _opt.max_batch_count = opt.max_batch_size / Node::size() as u64;
        create_dir_all(opt.dir.as_str())?;
        create_dir_all(opt.value_dir.as_str())?;
        // todo add directory lock
        if !(opt.value_log_file_size <= 2 << 30 && opt.value_log_file_size >= 1 << 20) {
            return Err(Error::ValueLogSize);
        }
        let (manifest_file, manifest) = open_or_create_manifest_file(opt.dir.as_str())?;
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
        let mut closers = Closers {
            update_size: Closer::new(0),
            compactors: Closer::new(0),
            mem_table: Closer::new(0),
            writes: Closer::new(0),
            value_gc: Closer::new(0),
        };
        // go out.updateSize(out.closers.updateSize)
        let mt = SkipList::new(arena_size(&opt));
        let mut out = KV {
            opt: opt.clone(),
            vlog: None,
            manifest,
            flush_chan: Channel::new(1),
            // write_chan: Channel::new(1),
            dir_lock_guard,
            value_dir_guard,
            closers,
            mt,
            imm: Vec::new(),
            last_used_cas_counter: Default::default(),
        };
        let mut vlog = ValueLogCore::default();
        vlog.open(&out, opt)?;

        out.vlog = Some(vlog);
        Ok(XArc::new(out))
    }

    // pub fn must_vlog(&self) -> &ValueLogCore {
    //     self.vlog.as_ref().unwrap()
    // }

    // pub fn must_mut_vlog(&mut self) -> &mut ValueLogCore {
    //     self.vlog.as_mut().unwrap()
    // }

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
    fn get_mem_tables(&self) -> Vec<&SkipList> {
        // TODO add kv lock
        let mut tables = Vec::with_capacity(self.imm.len() + 1);
        // Get mutable `mem_tables`.
        tables.push(&self.mt);
        tables[0].incr_ref();

        // Get immutable `mem_tables`.
        for tb in self.imm.iter().rev() {
            tb.incr_ref();
            tables.push(tb);
        }
        tables
    }

    fn write_requests(&self, reqs: &[Request]) -> Arc<Result<()>> {
        if reqs.is_empty() {
            return Arc::new(Ok(()));
        }
        let done = |res: Arc<Result<()>>| {
            for req in reqs {
                if res.is_err() {
                    // todo
                    *req.err.borrow_mut() = res.clone();
                }
                let worker = req.wait_group.borrow_mut().take().unwrap();
                worker.done();
            }
        };
        info!("write_requests called. Writing to value log");
        // CAS counter for all operations has to go onto value log. Otherwise, if it is just in
        // memtable for a long time, and following CAS operations use that as a check, when
        // replaying, we will think that these CAS operations should fail, when they are actually
        // valid.

        // There is code (in flush_mem_table) whose correctness depends on us generating CAS Counter
        // values _before_ we modify s.vptr here.
        for req in reqs {
            let counter_base = self.new_cas_counter(req.entries.len() as u64);
            for (idx, entry) in req.entries.iter().enumerate() {
                entry.borrow_mut().cas_counter = counter_base + idx as u64;
            }
        }

        let ok = self.vlog.as_ref().unwrap().write(reqs);
        if ok.is_err() {
            let _ok = Arc::new(ok);
            done(_ok.clone());
            return _ok.clone();
        }

        info!("Writing to memory table");
        let mut count = 0;
        for req in reqs {
            if req.entries.is_empty() {
                continue;
            }
            count += req.entries.len();
        }
        Arc::new(Ok(()))
    }

    async fn flush_mem_table(&self, lc: &Closer) -> Result<()> {
        while let Ok(task) = self.flush_chan.recv().await {
            if task.mt.is_none() {
                break;
            }

            if task.vptr.is_zero() {
                continue;
            }

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
            // todo
            task.mt.as_ref().unwrap().put(_HEAD, value);
        }

        Ok(())
    }

    fn new_cas_counter(&self, how_many: u64) -> u64 {
        self.last_used_cas_counter
            .fetch_add(how_many, Ordering::Relaxed)
    }
}

pub type WeakKV = XWeak<KV>;

pub type ArcKV = XArc<KV>;

impl Clone for WeakKV {
    fn clone(&self) -> Self {
        XWeak { x: self.x.clone() }
    }
}

fn write_level0_table(st: &SkipList, mut f: &File) -> Result<()> {
    let cur = st.new_cursor();
    let mut builder = Builder::default();
    while let Some(_) = cur.next() {
        let key = cur.key();
        let value = cur.value();
        builder.add(key, &value)?;
    }
    f.write_all(&builder.finish())?;
    Ok(())
}

fn arena_size(opt: &Options) -> usize {
    (opt.max_table_size + opt.max_batch_size + opt.max_batch_count * Node::size() as u64) as usize
}
