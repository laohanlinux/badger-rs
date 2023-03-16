use std::{collections::VecDeque, io::Cursor, sync::atomic::AtomicU64};
use std::ops::Deref;

use awaitgroup::WaitGroup;

use crate::types::{ArcMx, ArcRW};
use crate::{
    kv::KV,
    types::XArc,
    value_log::{MetaBit, ValuePointer},
    Decode, Result,
};

#[derive(Debug, PartialEq)]
pub(crate) enum PreFetchStatus {
    Empty,
    Prefetced,
}

// Retuned during iteration. Both the key() and value() output is only valid until
// iterator.next() is called.
pub(crate) struct KVItemInner {
    status: PreFetchStatus,
    kv: XArc<KV>,
    key: Vec<u8>,
    value: Vec<u8>,
    vptr: Vec<u8>,
    meta: u8,
    user_meta: u8,
    cas_counter: AtomicU64,
    wg: WaitGroup,
    err: Result<()>,
}

impl KVItemInner {
    // Returns the key. Remember to copy if you need to access it outside the iteration loop.
    pub(crate) fn key(&self) -> &[u8] {
        &self.value
    }

    pub(crate) async fn value(&self, consumer: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        self.wg.wait().await;
        if self.status == PreFetchStatus::Prefetced {
            if self.err.is_err() {
                return self.err.clone();
            }

            return consumer(&self.value);
        }

        Ok(())
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
        self.meta
    }
}

// Used to set options when iterating over Badger key-value stores.
pub(crate) struct IteratorOptions {
    // Indicates whether we should prefetch values during iteration and store them.
    pre_fetch_values: bool,
    // How may KV pairs to prefetch while iterating. Valid only if PrefetchValues is true.
    pre_fetch_size: isize,
    // Direction of iteration. False is forward, true is backward.
    reverse: bool,
}

pub(crate) const DEF_ITERATOR_OPTIONS: IteratorOptions = IteratorOptions {
    pre_fetch_size: 100,
    pre_fetch_values: true,
    reverse: false,
};

// Helps iterating over the KV pairs in a lexicographically sorted order.
struct IteratorExt {
    kv: XArc<KV>,
    opt: IteratorOptions,
    item: Option<XArc<KVItemInner>>,
    data: ArcRW<std::collections::LinkedList<XArc<KVItemInner>>>,
    waste: ArcRW<std::collections::LinkedList<XArc<KVItemInner>>>,
}

impl IteratorExt {
    fn new_item(&self) -> Option<XArc<KVItemInner>> {
        self.waste.write().pop_front()
    }

    // Returns pointer to the current KVItem.
    // This item is only valid until it.Next() gets called.
    fn item(&self) -> Option<XArc<KVItemInner>> {
        self.item.clone()
    }

    // Returns false when iteration is done.
    fn valid(&self) -> bool {
        self.item.is_some()
    }

    // Returns false when iteration is done
    // or when the current key is not prefixed by the specified prefix.
    fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.item.is_some() && self.item.as_ref().unwrap().key().starts_with(prefix)
    }

    // Close the iterator, It is important to call this when you're done with iteration.
    fn close(&self) {
        // TODO: We could handle this error.
        self.kv.vlog.as_ref().unwrap().deref();
    }
}
