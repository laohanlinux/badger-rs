use crate::options::Options;
use crate::SkipList;
use atomic::Atomic;
use crossbeam_epoch::Shared;
use drop_cell::defer;
use parking_lot::lock_api::RwLockWriteGuard;
use parking_lot::RawRwLock;
use std::borrow::Borrow;
use std::borrow::Cow::Owned;
use std::ops::Deref;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Arc;

type SkipListItem = crossbeam_epoch::Atomic<SkipList>;

pub struct SkipListManager {
    share_lock: parking_lot::RwLock<()>,
    mt: Option<SkipListItem>,
    imm: Arc<parking_lot::RwLock<Vec<SkipListItem>>>,
}

impl Default for SkipListManager {
    fn default() -> Self {
        SkipListManager {
            share_lock: parking_lot::RwLock::new(()),
            mt: None,
            imm: Arc::new(parking_lot::RwLock::new(vec![])),
        }
    }
}

impl SkipListManager {
    pub fn new(sz: usize) -> SkipListManager {
        SkipListManager {
            share_lock: parking_lot::RwLock::new(()),
            mt: Some(SkipListItem::new(SkipList::new(sz))),
            imm: Arc::new(parking_lot::RwLock::new(vec![])),
        }
    }

    pub fn take<'a>(&'a self, p: &'a crossbeam_epoch::Guard) -> Shared<'a, SkipList> {
        self.mt.as_ref().unwrap().load_consume(p)
    }

    pub fn mt_ref<'a>(&'a self, p: &'a crossbeam_epoch::Guard) -> Shared<'a, SkipList> {
        let st = self.mt.as_ref().unwrap().load(Ordering::Relaxed, &p);
        st
    }

    pub fn mt_clone(&self) -> SkipList {
        let p = crossbeam_epoch::pin();
        let mt = self.mt_ref(&p);
        unsafe { mt.as_ref().unwrap().clone() }
    }

    pub fn imm(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<SkipListItem>> {
        self.imm.write()
    }

    // TODO
    pub fn swap_st(&self, opt: Options) {
        self.lock_exclusive();
        defer! {self.unlock_exclusive()}
        let p = crossbeam_epoch::pin();
        let st = self.take(&p).into();
        self.imm.write().push(st);
        let st = SkipList::new(1000);
        self.mt
            .as_ref()
            .unwrap()
            .store(crossbeam_epoch::Owned::new(st), Ordering::Relaxed);
    }

    pub fn advance_imm(&self, mt: &SkipList) {
        self.lock_exclusive();
        defer! {self.unlock_exclusive()};
        let mut imm = self.imm();
        let first_imm = imm
            .first()
            .unwrap()
            .load(Ordering::Relaxed, &crossbeam_epoch::pin())
            .as_raw();
        assert!(ptr::eq(first_imm, mt));
        imm.remove(0);
    }

    pub fn lock_exclusive(&self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.share_lock.raw().lock_exclusive() }
    }

    pub fn unlock_exclusive(&self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.share_lock.raw().unlock_exclusive() }
    }
}

#[test]
fn ti() {}
