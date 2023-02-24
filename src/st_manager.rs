use crate::SkipList;
use atomic::Atomic;
use drop_cell::defer;
use parking_lot::lock_api::RwLockWriteGuard;
use parking_lot::RawRwLock;
use std::borrow::Borrow;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct SkipListManager {
    share_lock: parking_lot::RwLock<()>,
    mt: Atomic<NonNull<SkipList>>,
    imm: Arc<parking_lot::RwLock<Vec<NonNull<SkipList>>>>,
}

impl Default for SkipListManager {
    fn default() -> Self {
        todo!()
    }
}

impl SkipListManager {
    pub fn take(&self) -> NonNull<SkipList> {
        self.mt.swap(NonNull::dangling(), Ordering::Relaxed)
    }

    pub fn set(&self, st: NonNull<SkipList>) {
        self.mt.store(st, Ordering::Relaxed);
    }

    pub fn mt(&self) -> NonNull<SkipList> {
        self.mt.load(Ordering::Relaxed)
    }

    pub unsafe fn mt_ref(&self) -> &'_ SkipList {
        let st = self.mt();
        st.as_ref()
    }

    pub fn imm(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<NonNull<SkipList>>> {
        self.imm.write()
    }

    pub fn append_imm(&self, st: NonNull<SkipList>) {
        let mut imm = self.imm();
        imm.push(st);
    }

    pub fn swap_st(&self) {
        self.lock_exclusive();
        defer! {self.unlock_exclusive()}
        let st = self.take();
        self.append_imm(st);
        let st = Box::new(SkipList::new(1000));
        let ptr = st.as_ref() as *const SkipList as *mut SkipList;
        self.set(NonNull::new(ptr).unwrap());
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

// impl SkipListManager {
//     pub unsafe fn must_mt(&self) -> Option<&'_ Option<SkipList>> {
//         let p = &crossbeam_epoch::pin();
//         let mt = self.mt.load(Ordering::SeqCst, p);
//         mt.as_ref()
//     }
// }

#[test]
fn ti() {}
