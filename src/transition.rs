use std::collections::HashMap;
use std::sync::Arc;
use crate::{Result, KVItem, WeakKV, ValueStruct, DB};
use crate::iterator::{KVItemInner, PreFetchStatus};
use crate::value_log::{Entry, MetaBit};
use crate::y::compare_key;

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
}

impl TxN {
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, user_meta: u8) {
        let fp = farmhash::fingerprint64(&key); // Avoid dealing with byte arrays
        self.writes.write().map(|mut writes| writes.push(fp)).unwrap();
        let entry = Entry::default().key(key.clone()).value(value).user_meta(user_meta);
        self.pending_writes.write().map(|mut writes| writes.insert(key, entry)).unwrap();
    }

    pub fn delete(&self, key: &[u8]) {
        let fp = farmhash::fingerprint64(&key);
        self.writes.write().map(|mut writes| writes.push(fp)).unwrap();
        let entry = Entry::default().key(key.to_vec()).meta(MetaBit::BIT_DELETE.bits());
        self.pending_writes.write().map(|mut writes| writes.insert(key.to_vec(), entry)).unwrap();
    }

    pub async fn get(&self, key: &[u8]) -> Result<KVItem> {
        let kv_db = self.kv.upgrade().unwrap();
        let kv_db = DB::new(kv_db);
        let mut item = KVItem::from(KVItemInner::new(vec![], ValueStruct::default(), kv_db.clone()));
        if self.update {
            let pending_writes = self.pending_writes.write().unwrap();
            if let Some(e) = pending_writes.get(key) && &e.key == key {
                {
                    let mut wl = item.wl().await;
                    wl.meta = e.meta;
                    *wl.value.lock().await = e.value.clone();
                    wl.user_meta = e.user_meta;
                    wl.key = e.key.clone();
                    wl.status.store(PreFetchStatus::Prefetched, std::sync::atomic::Ordering::SeqCst);
                }
                // We probably don't need to set KV on item here.
                return Ok(item);
            }

            let fp = farmhash::fingerprint64(key);
            self.reads.write().map(|mut writes| writes.push(fp)).unwrap();
        }

        let seek = crate::y::key_with_ts(key, self.read_ts);
        kv_db.get_with_ext(&seek).await
    }

    pub async fn commit(&self) {

    }
}
