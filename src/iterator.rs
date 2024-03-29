use crate::iterator::PreFetchStatus::Prefetched;
use crate::kv::_BADGER_PREFIX;
use crate::types::{ArcRW, Channel, Closer, TArcMx, TArcRW};
use crate::{hex_str, ValueStruct, KV};
use crate::{
    value_log::{MetaBit, ValuePointer},
    Decode, MergeIterator, Result, Xiterator, EMPTY_SLICE,
};

use atomic::Atomic;

use std::fmt::{Debug, Display, Formatter, Pointer};
use std::future::Future;

use std::pin::{pin, Pin};

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{io::Cursor, sync::atomic::AtomicU64};
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum PreFetchStatus {
    Empty,
    Prefetched,
}

#[derive(Clone, Debug)]
pub struct KVItem {
    inner: TArcRW<KVItemInner>,
}

impl From<KVItemInner> for KVItem {
    fn from(value: KVItemInner) -> Self {
        Self {
            inner: TArcRW::new(tokio::sync::RwLock::new(value)),
        }
    }
}
// impl Deref for KVItem {
//     type Target = tokio::sync::RwLock<KVItemInner>;
//
//     fn deref(&self) -> &Self::Target {
//         self.inner.as_ref()
//     }
// }

impl KVItem {
    pub async fn key(&self) -> Vec<u8> {
        let inner = self.rl().await;
        inner.key().to_vec()
    }

    pub async fn value(&self) -> Result<Vec<u8>> {
        let inner = self.rl().await;
        inner.get_value().await
    }

    pub async fn has_value(&self) -> bool {
        let inner = self.rl().await;
        inner.has_value()
    }

    pub async fn counter(&self) -> u64 {
        let inner = self.rl().await;
        inner.counter()
    }

    pub async fn user_meta(&self) -> u8 {
        let inner = self.rl().await;
        inner.user_meta()
    }

    pub(crate) async fn rl(&self) -> RwLockReadGuard<'_, KVItemInner> {
        self.inner.read().await
    }

    pub(crate) async fn wl(&self) -> RwLockWriteGuard<'_, KVItemInner> {
        self.inner.write().await
    }
}

// Returned during iteration. Both the key() and value() output is only valid until
// iterator.next() is called.
#[derive(Clone)]
pub(crate) struct KVItemInner {
    status: Arc<Atomic<PreFetchStatus>>,
    kv: KV,
    key: Vec<u8>,
    // TODO, Opz memory
    vptr: Vec<u8>,
    value: TArcMx<Vec<u8>>,
    meta: u8,
    user_meta: u8,
    cas_counter: Arc<AtomicU64>,
    wg: Closer,
    err: Result<()>,
}

impl Display for KVItemInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("kv")
            .field("key", &hex_str(&self.key))
            .field("meta", &self.meta)
            .field("user_meta", &self.user_meta)
            .field("cas", &self.counter())
            .finish()
    }
}

impl Debug for KVItemInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("kv")
            .field("key", &hex_str(&self.key))
            .field("meta", &self.meta)
            .field("user_meta", &self.user_meta)
            .field("cas", &self.counter())
            .finish()
    }
}

impl KVItemInner {
    pub(crate) fn new(key: Vec<u8>, value: ValueStruct, kv: KV) -> KVItemInner {
        Self {
            status: Arc::new(Atomic::new(PreFetchStatus::Empty)),
            kv,
            key,
            value: TArcMx::new(Default::default()),
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

    // Return value
    pub(crate) async fn get_value(&self) -> Result<Vec<u8>> {
        let ch = Channel::new(1);
        self.value(|value| {
            let tx = ch.tx();
            let value = value.to_vec();
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
        mut consumer: impl FnMut(&[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        // Wait result
        self.wg.wait().await;
        if self.status.load(Ordering::Acquire) == Prefetched {
            if self.err.is_err() {
                return self.err.clone();
            }
            let value = self.value.lock().await;
            return if value.is_empty() {
                consumer(&EMPTY_SLICE).await
            } else {
                consumer(&value).await
            };
        }
        return self.kv.yield_item_value(self.clone(), consumer).await;
    }

    pub(crate) fn has_value(&self) -> bool {
        if self.meta == 0 && self.vptr.is_empty() {
            return false;
        }
        if (self.meta & MetaBit::BIT_DELETE.bits()) > 0 {
            return false;
        }
        true
    }

    // async fetch value from value_log.
    pub(crate) async fn pre_fetch_value(&self) -> Result<()> {
        let kv = self.kv.clone();
        kv.yield_item_value(self.clone(), |value| {
            let status_wl = self.status.clone();
            let value = value.to_vec();
            let value_wl = self.value.clone();
            Box::pin(async move {
                status_wl.store(Prefetched, Ordering::Release);
                if value.is_empty() {
                    return Ok(());
                }
                let mut value_wl = value_wl.lock().await;
                *value_wl = value;
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
        self.cas_counter.load(atomic::Ordering::Acquire)
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
#[derive(Debug, Clone, Copy)]
pub struct IteratorOptions {
    // Indicates whether we should prefetch values during iteration and store them.
    pub(crate) pre_fetch_values: bool,
    // How may KV pairs to prefetch while iterating. Valid only if PrefetchValues is true.
    pub(crate) pre_fetch_size: isize,
    // Direction of iteration. False is forward, true is backward.
    pub(crate) reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        DEF_ITERATOR_OPTIONS
    }
}

impl IteratorOptions {
    pub fn new(pre_fetch_values: bool, pre_fetch_size: isize, reverse: bool) -> Self {
        IteratorOptions {
            pre_fetch_values,
            pre_fetch_size,
            reverse,
        }
    }
}

pub(crate) const DEF_ITERATOR_OPTIONS: IteratorOptions = IteratorOptions {
    pre_fetch_size: 100,
    pre_fetch_values: true,
    reverse: false,
};

/// Helps iterating over the KV pairs in a lexicographically sorted order.
/// skiplist,     sst      vlog
///  |             |        |
///  |             |        |
///  IteratorExt  reference
pub struct IteratorExt {
    kv: KV,
    itr: MergeIterator,
    opt: IteratorOptions,
    item: ArcRW<Option<KVItem>>,
    // Cache the prefetch keys, not inlcude current value
    data: ArcRW<std::collections::LinkedList<KVItem>>,
    has_rewind: ArcRW<bool>,
}

/// TODO FIXME
// impl futures_core::Stream for IteratorExt {
//     type Item = KVItem;
//
//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         let mut has_rewind = self.has_rewind.write();
//         if !*has_rewind {
//             *has_rewind = true;
//             match Pin::new(&mut pin!(self.rewind())).poll(cx) {
//                 std::task::Poll::Pending => {
//                     warn!("<<<<Pending>>>>>");
//                     std::task::Poll::Pending
//                 }
//                 std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
//                 std::task::Poll::Ready(t) => std::task::Poll::Ready(t),
//             }
//         } else {
//             match Pin::new(&mut pin!(self.next())).poll(cx) {
//                 std::task::Poll::Pending => {
//                     warn!("<<<<Pending>>>>>");
//                     std::task::Poll::Pending
//                 }
//                 std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
//                 std::task::Poll::Ready(t) => std::task::Poll::Ready(t),
//             }
//         }
//     }
// }

impl IteratorExt {
    pub(crate) fn new(kv: KV, itr: MergeIterator, opt: IteratorOptions) -> IteratorExt {
        IteratorExt {
            kv,
            opt,
            itr,
            data: ArcRW::default(),
            item: Arc::new(Default::default()),
            has_rewind: ArcRW::default(),
        }
    }

    // pub(crate) async fn new_async_iterator(
    //     kv: KV,
    //     itr: MergeIterator,
    //     opt: IteratorOptions,
    // ) -> Box<dyn futures_core::Stream<Item=KVItem>> {
    //     let itr = Self::new(kv, itr, opt);
    //     Box::new(itr)
    // }

    // Seek to the provided key if present. If absent, if would seek to the next smallest key
    // greater than provided if iterating in the forward direction. Behavior would be reversed is
    // iterating backwards.
    pub async fn seek(&self, key: &[u8]) -> Option<KVItem> {
        while let Some(el) = self.data.write().pop_front() {
            el.rl().await.wg.wait().await;
        }
        while let Some(el) = self.itr.seek(key) {
            if el.key().starts_with(_BADGER_PREFIX) {
                continue;
            }
            break;
        }
        self.pre_fetch().await;
        self.item.read().clone()
    }

    // Rewind the iterator cursor all the wy to zero-th position, which would be the
    // smallest key if iterating forward, and largest if iterating backward. It dows not
    // keep track of whether the cursor started with a `seek`.
    pub async fn rewind(&self) -> Option<KVItem> {
        while let Some(el) = self.data.write().pop_front() {
            // Just cleaner to wait before pushing. No ref counting need.
            el.rl().await.wg.wait().await;
        }
        // rewind the iterator
        // rewind, next, rewind?, thie item is who!
        let mut item = self.itr.rewind();
        // filter internal data
        while item.is_some() && item.as_ref().unwrap().key().starts_with(_BADGER_PREFIX) {
            item = self.itr.next();
        }
        // Before every rewind, the item will be reset to None
        self.item.write().take();
        // prefetch item.
        self.pre_fetch().await;
        // return the first el.
        self.item.read().clone()
    }

    // Advance the iterator by one (*NOTICE*: must be rewind when you call self.next())
    pub async fn next(&self) -> Option<KVItem> {
        // Ensure current item has load
        if let Some(el) = self.item.write().take() {
            el.rl().await.wg.wait().await; // Just cleaner to wait before pushing to avoid doing ref counting.
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

        let xitem = self.new_item();
        self.fill(xitem.clone()).await;
        self.data.write().push_back(xitem.clone());
        Some(xitem)
    }

    pub async fn peek(&self) -> Option<KVItem> {
        self.item.read().clone()
    }
}

impl IteratorExt {
    // Returns false when iteration is done
    // or when the current key is not prefixed by the specified prefix.
    async fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.item.read().is_some()
            && self
                .item
                .read()
                .as_ref()
                .unwrap()
                .rl()
                .await
                .key()
                .starts_with(prefix)
    }

    // Close the iterator, It is important to call this when you're done with iteration.
    pub async fn close(&self) -> Result<()> {
        // TODO: We could handle this error.
        self.kv.vlog.as_ref().unwrap().decr_iterator_count().await?;
        Ok(())
    }

    // fill the value
    async fn fill(&self, item: KVItem) {
        let vs = self.itr.peek().unwrap();
        let vs = vs.value();
        {
            let mut item = item.wl().await;
            item.meta = vs.meta;
            item.user_meta = vs.user_meta;
            item.cas_counter.store(vs.cas_counter, Ordering::Release);
            item.key.extend(self.itr.peek().as_ref().unwrap().key());
            item.vptr.extend(&vs.value);
            item.value.lock().await.clear();
        }

        // need fetch value, use new coroutine to load value.
        if self.opt.pre_fetch_values {
            item.rl().await.wg.add_running(1);
            tokio::spawn(async move {
                // FIXME we are not handling errors here.
                {
                    let item = item.rl().await;
                    if let Err(err) = item.pre_fetch_value().await {
                        log::error!("Failed to fetch value, {}", err);
                    }
                }
                item.rl().await.wg.done();
            });
        }
    }

    // Prefetch load items.
    async fn pre_fetch(&self) {
        let mut pre_fetch_size = 2;
        if self.opt.pre_fetch_values && self.opt.pre_fetch_size > 1 {
            pre_fetch_size = self.opt.pre_fetch_size;
        }

        let itr = &self.itr;
        let mut count = 0;
        while let Some(item) = itr.peek() {
            if item.key().starts_with(crate::kv::_BADGER_PREFIX) {
                itr.next();
                continue;
            }
            if item.value().meta & MetaBit::BIT_DELETE.bits() > 0 {
                itr.next();
                continue;
            }
            count += 1;
            let xitem = self.new_item();
            // fill a el from itr.peek
            self.fill(xitem.clone()).await;
            if self.item.read().is_none() {
                self.item.write().replace(xitem); // store it
            } else {
                // push prefetch el into cache queue, Notice it not including current item
                self.data.write().push_back(xitem);
            }
            if count == pre_fetch_size {
                break;
            }
            itr.next();
        }
    }

    fn new_item(&self) -> KVItem {
        let inner_item = KVItemInner {
            status: Arc::new(Atomic::new(PreFetchStatus::Empty)),
            kv: self.kv.clone(),
            key: vec![],
            value: TArcMx::new(Default::default()),
            vptr: vec![],
            meta: 0,
            user_meta: 0,
            cas_counter: Arc::new(Default::default()),
            wg: Closer::new("IteratorExt".to_owned()),
            err: Ok(()),
        };
        return KVItem::from(inner_item);
    }

    // Returns false when iteration is done.
    fn valid(&self) -> bool {
        self.item.read().is_some()
    }
}
