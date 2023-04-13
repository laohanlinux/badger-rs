use crate::iterator::PreFetchStatus::Prefetched;
use crate::kv::_BADGER_PREFIX;
use crate::types::{ArcMx, ArcRW, Channel, Closer, TArcMx, TArcRW};
use crate::{
    kv::KV,
    types::XArc,
    value_log::{MetaBit, ValuePointer},
    Decode, Result, Xiterator,
};
use crate::{MergeIterOverIterator, ValueStruct};
use log::Metadata;
use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::{io::Cursor, sync::atomic::AtomicU64};
use tokio::io::AsyncWriteExt;
use tokio::time::Sleep;

#[derive(Debug, PartialEq)]
pub(crate) enum PreFetchStatus {
    Empty,
    Prefetched,
}

pub(crate) type KVItem = TArcRW<KVItemInner>;

// Returned during iteration. Both the key() and value() output is only valid until
// iterator.next() is called.
#[derive(Clone)]
pub(crate) struct KVItemInner {
    status: TArcRW<PreFetchStatus>,
    kv: XArc<KV>,
    key: Vec<u8>,
    value: TArcRW<Vec<u8>>,
    vptr: Vec<u8>,
    meta: u8,
    user_meta: u8,
    cas_counter: Arc<AtomicU64>,
    wg: Closer,
    err: Result<()>,
}

impl KVItemInner {
    pub(crate) fn new(key: Vec<u8>, value: ValueStruct, kv: XArc<KV>) -> KVItemInner {
        Self {
            status: Arc::new(tokio::sync::RwLock::new(PreFetchStatus::Empty)),
            kv,
            key,
            value: Arc::new(Default::default()),
            vptr: value.value,
            meta: value.meta,
            user_meta: value.user_meta,
            cas_counter: Arc::new(AtomicU64::new(value.cas_counter)),
            wg: Closer::new("kv".to_owned()),
            err: Ok(()),
        }
    }

    // Returns the key. Remember to copy if you need to access it outside the iteration loop.
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    pub async fn get_value(&self) -> Result<Vec<u8>> {
        let ch = Channel::new(1);
        self.value(|value| {
            let tx = ch.tx();
            Box::pin(async move {
                tx.send(value).await.unwrap();
                Ok(())
            })
        })
        .await?;
        Ok(ch.recv().await.unwrap())
    }

    // Value retrieves the value of the item from the value log. It calls the
    // consumer function with a slice argument representing the value. In case
    // of error, the consumer function is not called.
    //
    // Note that the call to the consumer func happens synchronously.
    pub(crate) async fn value(
        &self,
        mut consumer: impl FnMut(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        // Wait result
        self.wg.wait().await;
        if *self.status.read().await == PreFetchStatus::Prefetched {
            if self.err.is_err() {
                return self.err.clone();
            }
            return consumer(self.value.read().await.clone()).await;
        }
        return self.kv.yield_item_value(self.clone(), consumer).await;
    }

    pub(crate) fn has_value(&self) -> bool {
        if self.meta == 0 && self.vptr.is_empty() {
            return false;
        }
        if self.meta & MetaBit::BIT_DELETE.bits() > 0 {
            return false;
        }

        true
    }

    // async fetch value from value_log.
    pub(crate) async fn pre_fetch_value(&self) -> Result<()> {
        let kv = self.kv.clone();
        kv.yield_item_value(self.clone(), |value| {
            let ref_status = self.status.clone();
            let ref_value = self.value.clone();
            Box::pin(async move {
                if value.is_empty() {
                    *ref_status.write().await = PreFetchStatus::Prefetched;
                    return Ok(());
                }
                ref_value.write().await.extend(value);
                *ref_status.write().await = PreFetchStatus::Prefetched;
                Ok(())
            })
        })
        .await
    }

    // Returns approximate size of the key-value pair.
    //
    // This can be called while iterating through a store to quickly estimate the
    // size of a range of key-value pairs (without fetching the corresponding)
    // values).
    pub(crate) fn estimated_size(&self) -> u64 {
        if !self.has_value() {
            return 0;
        }
        if self.meta & MetaBit::BIT_VALUE_POINTER.bits() == 0 {
            return (self.key.len() + self.vptr.len()) as u64;
        }
        let mut vpt = ValuePointer::default();
        vpt.dec(&mut Cursor::new(&self.vptr)).unwrap();
        vpt.len as u64 // includes key length
    }

    // Returns the  CAS counter associated with the value.
    pub(crate) fn counter(&self) -> u64 {
        self.cas_counter.load(atomic::Ordering::Relaxed)
    }

    // Returns the user_meta set by the user. Typically, this byte, optionally set by the user
    // is used to interpret the value.
    pub(crate) fn user_meta(&self) -> u8 {
        self.user_meta
    }

    pub(crate) fn meta(&self) -> u8 {
        self.meta
    }

    pub(crate) fn vptr(&self) -> &[u8] {
        &self.vptr
    }
}

// Used to set options when iterating over Badger key-value stores.
pub(crate) struct IteratorOptions {
    // Indicates whether we should prefetch values during iteration and store them.
    pub(crate) pre_fetch_values: bool,
    // How may KV pairs to prefetch while iterating. Valid only if PrefetchValues is true.
    pub(crate) pre_fetch_size: isize,
    // Direction of iteration. False is forward, true is backward.
    pub(crate) reverse: bool,
}

pub(crate) const DEF_ITERATOR_OPTIONS: IteratorOptions = IteratorOptions {
    pre_fetch_size: 100,
    pre_fetch_values: true,
    reverse: false,
};

// Helps iterating over the KV pairs in a lexicographically sorted order.
pub(crate) struct IteratorExt {
    kv: XArc<KV>,
    itr: MergeIterOverIterator,
    opt: IteratorOptions,
    item: ArcRW<Option<KVItem>>,
    data: ArcRW<std::collections::LinkedList<KVItem>>,
}

impl IteratorExt {
    pub(crate) fn new(
        kv: XArc<KV>,
        itr: MergeIterOverIterator,
        opt: IteratorOptions,
    ) -> IteratorExt {
        IteratorExt {
            kv,
            opt,
            itr,
            data: ArcRW::default(),
            item: Arc::new(Default::default()),
        }
    }

    // Seek to the provided key if present. If absent, if would seek to the next smallest key
    // greater than provided if iterating in the forward direction. Behavior would be reversed is
    // iterating backwards.
    pub(crate) async fn seek(&self, key: &[u8]) -> Option<KVItem> {
        while let Some(el) = self.data.write().pop_front() {
            el.read().await.wg.wait().await;
        }
        while let Some(el) = self.itr.seek(key) {
            if el.key().starts_with(_BADGER_PREFIX) {
                continue;
            }
        }
        self.pre_fetch().await;
        self.item.read().clone()
    }

    // Rewind the iterator cursor all the wy to zero-th position, which would be the
    // smallest key if iterating forward, and largest if iterating backward. It dows not
    // keep track of whether the cusor started with a `seek`.
    pub(crate) async fn rewind(&self) -> Option<KVItem> {
        while let Some(el) = self.data.write().pop_front() {
            // Just cleaner to wait before pushing. No ref counting need.
            el.read().await.wg.wait().await;
        }

        let mut item = self.itr.rewind();
        while item.is_some() && item.as_ref().unwrap().key().starts_with(_BADGER_PREFIX) {
            item = self.itr.next();
        }
        self.pre_fetch().await;
        self.item.read().clone()
    }

    // Advance the iterator by one. Always check it.valid() after a next ()
    // to ensure you have access to a valid it.item()
    pub(crate) async fn next(&self) -> Option<KVItem> {
        // Ensure current item
        if let Some(el) = self.item.write().take() {
            el.read().await.wg.wait().await; // Just cleaner to wait before pushing to avoid doing ref counting.
        }
        // Set next item to current
        if let Some(el) = self.data.write().pop_front() {
            self.item.write().replace(el);
        }
        // Advance internal iterator until entry is not deleted
        while let Some(el) = self.itr.next() {
            if el.key().starts_with(_BADGER_PREFIX) {
                continue;
            }
            if el.value().meta & MetaBit::BIT_DELETE.bits() == 0 {
                // Not deleted
                break;
            }
        }
        let item = self.itr.peek();
        if item.is_none() {
            return None;
        }
        let mut xitem = self.new_item();
        self.fill(xitem.clone()).await;
        self.data.write().push_back(xitem.clone());
        Some(xitem)
    }
}

impl IteratorExt {
    fn new_item(&self) -> KVItem {
        let inner_item = KVItemInner {
            status: Arc::new(tokio::sync::RwLock::new(PreFetchStatus::Empty)),
            kv: self.kv.clone(),
            key: vec![],
            value: Arc::new(Default::default()),
            vptr: vec![],
            meta: 0,
            user_meta: 0,
            cas_counter: Arc::new(Default::default()),
            wg: Closer::new("IteratorExt".to_owned()),
            err: Ok(()),
        };
        return KVItem::new(tokio::sync::RwLock::new(inner_item));
    }

    // Returns pointer to the current KVItem.
    // This item is only valid until it.Next() gets called.
    fn item(&self) -> Option<KVItem> {
        todo!()
        //self.item.clone()
    }

    // Returns false when iteration is done.
    fn valid(&self) -> bool {
        self.item.read().is_some()
    }

    // Returns false when iteration is done
    // or when the current key is not prefixed by the specified prefix.
    async fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.item.read().is_some()
            && self
                .item
                .read()
                .as_ref()
                .unwrap()
                .read()
                .await
                .key()
                .starts_with(prefix)
    }

    // Close the iterator, It is important to call this when you're done with iteration.
    fn close(&self) -> Result<()> {
        // TODO: We could handle this error.
        self.kv.vlog.as_ref().unwrap().decr_iterator_count()?;
        Ok(())
    }

    async fn fill(&self, item: KVItem) {
        let vs = self.itr.peek().unwrap();
        let vs = vs.value();
        {
            let mut item = item.write().await;
            item.meta = vs.meta;
            item.user_meta = vs.user_meta;
            item.cas_counter.store(vs.cas_counter, Ordering::Relaxed);
            item.key.extend(self.itr.peek().as_ref().unwrap().key());
            item.vptr.extend(&vs.value);
            item.value.write().await.clear();
        }

        // need fetch value, use new coroutine to load value.
        if self.opt.pre_fetch_values {
            item.read().await.wg.add_running(1);
            tokio::spawn(async move {
                // FIXME we are not handling errors here.
                {
                    let item = item.read().await;
                    if let Err(err) = item.pre_fetch_value().await {
                        log::error!("Failed to fetch value, {}", err);
                    }
                }
                item.read().await.wg.done();
            });
        }
    }

    async fn pre_fetch(&self) {
        let mut pre_fetch_size = 2;
        if self.opt.pre_fetch_values && self.opt.pre_fetch_size > 1 {
            pre_fetch_size = self.opt.pre_fetch_size;
        }

        let itr = &self.itr;
        let mut count = 0;
        while let Some(item) = itr.next() {
            if item.key().starts_with(crate::kv::_BADGER_PREFIX) {
                continue;
            }
            if item.value().meta & MetaBit::BIT_DELETE.bits() > 0 {
                continue;
            }
            count += 1;
            let xitem = self.new_item();
            self.fill(xitem.clone()).await;
            if self.item.read().is_none() {
                self.item.write().replace(xitem);
            } else {
                self.data.write().push_back(xitem);
            }
            if count == pre_fetch_size {
                break;
            }
        }
    }
}
