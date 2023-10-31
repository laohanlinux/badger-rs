use crate::iterator::{KVItemInner, PreFetchStatus};
use crate::value_log::{Entry, MetaBit};
use crate::y::compare_key;
use crate::{Error, KVItem, Result, ValueStruct, WeakKV, DB};
use drop_cell::defer;
use parking_lot::RwLock;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

struct GlobalTxNStateInner {
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

        for ro in tx_reads.iter() {
            if let Some(ts) = self.commits.get(ro) && *ts > tx.read_ts {
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
    read_ts: u64,
    // update is used to conditionally keep track of reads.
    update: bool,
    // contains fingerprints of keys read.
    reads: Arc<std::sync::RwLock<Vec<u64>>>,
    // contains fingerprints of keys written.
    writes: Arc<std::sync::RwLock<Vec<u64>>>,
    // cache stores any writes done by txn.
    pending_writes: Arc<std::sync::RwLock<HashMap<Vec<u8>, Entry>>>,

    kv: WeakKV,
    gs: Arc<std::sync::RwLock<GlobalTxNStateInner>>,
}

impl TxN {
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
        let kv_db = self.kv.upgrade().unwrap();
        let kv_db = DB::new(kv_db);
        let item = KVItem::from(KVItemInner::new(
            vec![],
            ValueStruct::default(),
            kv_db.clone(),
        ));
        if self.update {
            let pending_writes = self.pending_writes.write().unwrap();
            if let Some(e) = pending_writes.get(key) && &e.key == key {
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

            let fp = farmhash::fingerprint64(key);
            self.reads
                .write()
                .map(|mut writes| writes.push(fp))
                .unwrap();
        }

        let seek = crate::y::key_with_ts(key, self.read_ts);
        kv_db.get_with_ext(&seek).await
    }

    pub async fn commit(&self) -> Result<()> {
        if self.writes.write().unwrap().is_empty() {
            return Ok(()); // Ready only translation.
        }
        let commit_ts = self.gs.write().unwrap().new_commit_ts(&self);
        if commit_ts == 0 {
            return Err(Error::TxCommitConflict);
        }
        Ok(())
    }
}

#[test]
fn it() {}
