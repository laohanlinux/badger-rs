use crate::iterator::{KVItemInner, PreFetchStatus};
use crate::table::iterator::IteratorItem;
use crate::types::TArcRW;
use crate::value_log::{Entry, MetaBit};
use crate::y::compare_key;
use crate::{
    Error, IteratorExt, IteratorOptions, KVItem, MergeIterOverBuilder, Result, UniIterator,
    ValueStruct, Xiterator, DB, TXN_KEY,
};
use drop_cell::defer;
use parking_lot::RwLock;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone, Default)]
pub struct GlobalTxNState {
    inner: TArcRW<GlobalTxNStateInner>,
}

impl GlobalTxNState {
    pub async fn rl(&self) -> RwLockReadGuard<'_, GlobalTxNStateInner> {
        self.inner.read().await
    }
    pub async fn wl(&self) -> RwLockWriteGuard<'_, GlobalTxNStateInner> {
        self.inner.write().await
    }

    pub async fn read_ts(&self) -> u64 {
        self.rl().await.read_ts()
    }
}

struct GlobalTxNStateInner {
    // TODO may be not lock, because outline has locked a time
    lock: Arc<RwLock<()>>,
    cur_read: AtomicU64,
    next_commit: AtomicU64,
    // These two structures are used to figure out when a commit is done. The minimum done commit is
    // used to update cur_read.
    commit_mark: BinaryHeap<u64>,
    pending_commits: HashSet<u64>,

    // commits stores a key fingerprint and latest commit counter for it.
    commits: HashMap<u64, u64>,
}

impl Default for GlobalTxNStateInner {
    fn default() -> Self {
        GlobalTxNStateInner {
            lock: Default::default(),
            cur_read: Default::default(),
            next_commit: AtomicU64::new(1),
            commit_mark: Default::default(),
            pending_commits: Default::default(),
            commits: Default::default(),
        }
    }
}

impl GlobalTxNStateInner {
    fn read_ts(&self) -> u64 {
        self.cur_read.load(Ordering::SeqCst)
    }

    // must be called while having a lock.
    fn has_conflict(&self, tx: &TxN) -> bool {
        let tx_reads = tx.reads.write().unwrap();
        if tx_reads.is_empty() {
            return false;
        }
        // check the all tx keys version must be more than `commits`
        for ro in tx_reads.iter() {
            if let Some(ts) = self.commits.get(ro)
                && *ts > tx.read_ts
            {
                return true;
            }
        }
        false
    }

    fn new_commit_ts(&mut self, tx: &TxN) -> u64 {
        let _lock = self.lock.write();

        if self.has_conflict(tx) {
            return 0;
        }

        let ts = self.next_commit.load(Ordering::SeqCst);
        let tx_writes = tx.writes.write().unwrap();
        for w in tx_writes.iter() {
            // Update the commit_ts.
            self.commits.insert(*w, ts);
        }
        self.commit_mark.push(ts);
        if self.pending_commits.contains(&ts) {
            panic!("We shouldn't have the commit ts: {}", ts);
        }

        self.pending_commits.insert(ts);
        self.next_commit.fetch_add(1, Ordering::SeqCst);
        ts
    }

    fn done_commit(&mut self, cts: u64) {
        let _lock = self.lock.write();
        if !self.pending_commits.remove(&cts) {
            panic!("We should already have the commit ts: {}", cts);
        }

        let mut min = 0;
        while !self.commit_mark.is_empty() {
            let ts = self.commit_mark.peek().unwrap();
            if self.pending_commits.contains(ts) {
                // Still waiting for a txn to commit.
                break;
            }
            min = *ts;
            self.commit_mark.pop();
        }

        if min == 0 {
            return;
        }

        self.cur_read.store(min, Ordering::SeqCst);
        self.next_commit.store(min + 1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct TxN {
    // the read operation ts, it also was the version of key.
    pub(crate) read_ts: u64,
    // update is used to conditionally keep track of reads.
    pub(crate) update: bool,
    // contains fingerprints of keys read.
    pub(crate) reads: Arc<std::sync::RwLock<Vec<u64>>>,
    // contains fingerprints(hash64(key)) of keys written.
    pub(crate) writes: Arc<std::sync::RwLock<Vec<u64>>>,
    // cache stores any writes done by txn.
    pub(crate) pending_writes: Arc<std::sync::RwLock<HashMap<Vec<u8>, Entry>>>,

    pub(crate) kv: DB,
}

impl TxN {
    // set a key, value with user meta at the tx
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) {
        let fp = farmhash::fingerprint64(&key); // Avoid dealing with byte arrays
        self.writes
            .write()
            .map(|mut writes| writes.push(fp))
            .unwrap();
        let entry = Entry::default()
            .key(key.clone())
            .value(value)
            .user_meta(user_meta);
        self.pending_writes
            .write()
            .map(|mut writes| writes.insert(key, entry))
            .unwrap();
    }

    // delete a key with at the tx
    pub fn delete(&self, key: &[u8]) {
        let fp = farmhash::fingerprint64(&key);
        self.writes
            .write()
            .map(|mut writes| writes.push(fp))
            .unwrap();
        let entry = Entry::default()
            .key(key.to_vec())
            .meta(MetaBit::BIT_DELETE.bits());
        self.pending_writes
            .write()
            .map(|mut writes| writes.insert(key.to_vec(), entry))
            .unwrap();
    }

    pub async fn get(&self, key: &[u8]) -> Result<KVItem> {
        let kv_db = self.get_kv();
        let item = KVItem::from(KVItemInner::new(
            vec![],
            ValueStruct::default(),
            kv_db.clone(),
        ));
        // if the transaction is writeable, Prioritize reading local writes
        if self.update {
            let pending_writes = self.pending_writes.write().unwrap();
            if let Some(e) = pending_writes.get(key)
                && &e.key == key
            {
                // Fulfill from cache.
                {
                    let mut wl = item.wl().await;
                    wl.meta = e.meta;
                    *wl.value.lock().await = e.value.clone();
                    wl.user_meta = e.user_meta;
                    wl.key = e.key.clone();
                    *wl.status.write().unwrap() = PreFetchStatus::Prefetched;
                }
                // We probably don't need to set KV on item here.
                return Ok(item);
            }

            // Notice: if not found at local writes, need to read operation because they key seek from DB below.
            let fp = farmhash::fingerprint64(key);
            self.reads
                .write()
                .map(|mut writes| writes.push(fp))
                .unwrap();
        }
        // Seek from DB
        let seek = crate::y::key_with_ts(key, self.read_ts);
        kv_db.get_with_ext(&seek).await
    }

    pub async fn commit(&self) -> Result<()> {
        if self.writes.write().unwrap().is_empty() {
            return Ok(()); // Ready only translation.
        }
        // TODO FIXME lock
        let gs = self.get_kv().txn_state.clone();
        let mut gs = gs.inner.write().await;
        // Get a new commit ts for current tx
        let commit_ts = gs.new_commit_ts(&self);
        if commit_ts == 0 {
            return Err(Error::TxCommitConflict);
        }

        let mut entries = vec![];
        let mut pending_writes = self.pending_writes.write().unwrap();
        for e in pending_writes.values_mut().into_iter() {
            // Suffix the keys with commit ts, so the key versions are sorted in descending order of commit timestamp.
            // descending order of commit timestamp.
            let ts_key = crate::y::key_with_ts(&e.key, commit_ts);
            e.key.clear();
            e.key.extend_from_slice(&ts_key);
            entries.push(e.clone());
        }
        // TODO: Add logic in replay to deal with this.
        let entry = Entry::default()
            .key(TXN_KEY.to_vec())
            .value(commit_ts.to_string().as_bytes().to_vec())
            .meta(MetaBit::BIT_FIN_TXN.bits());
        entries.push(entry);

        // TODO if has some key failed to execute .....?
        defer! {gs.done_commit(commit_ts)};

        let db = self.get_kv();
        let res = db.batch_set(entries).await;

        for ret in res {
            ret?;
        }

        Ok(())
    }

    fn new_iterator(&self, opt: IteratorOptions) -> IteratorExt {
        let db = self.get_kv();
        // Notice, the iterator is global iterator, so must incr reference for memtable(SikpList), sst(file), vlog(file).
        let p = crossbeam_epoch::pin();
        let tables = db.get_mem_tables(&p);
        // TODO it should decr at IteratorExt close.
        defer! {
            tables.iter().for_each(|table| unsafe {table.as_ref().unwrap().decr_ref()});
        }
        // add vlog reference.
        db.must_vlog().incr_iterator_count();

        // Create iterators across all the tables involved first.
        let mut itrs: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
        for tb in tables.clone() {
            let st = unsafe { tb.as_ref().unwrap().clone() };
            let iter = Box::new(UniIterator::new(st, opt.reverse));
            itrs.push(iter);
        }
        // Extend sst.table
        itrs.extend(db.must_lc().as_iterator(opt.reverse));
        let mitr = MergeIterOverBuilder::default().add_batch(itrs).build();
        let txn = self as *const TxN;
        IteratorExt::new(txn, mitr, opt)
    }

    pub(crate) fn get_kv(&self) -> DB {
        self.kv.clone()
    }
}

pub(crate) struct BoxTxN {
    pub tx: *const TxN,
}

unsafe impl Send for BoxTxN {}

unsafe impl Sync for BoxTxN {}

impl BoxTxN {
    pub(crate) fn new(tx: *const TxN) -> BoxTxN {
        BoxTxN { tx }
    }
}
