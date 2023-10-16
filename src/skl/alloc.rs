use std::cell::{Cell, UnsafeCell};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{size_of, ManuallyDrop};
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::Node;
use std::ptr;

pub(crate) const PtrAlign: usize = 7;

pub trait Allocate: Send + Sync {
    #[inline]
    fn alloc(&self, size: usize) -> usize;
    #[inline]
    fn size(&self) -> usize;
    #[inline]
    unsafe fn get_mut<T>(&self, offset: usize) -> *mut T;
    #[inline]
    fn offset<T>(&self, ptr: *const T) -> usize;
    #[inline]
    fn len(&self) -> usize;
    #[inline]
    fn cap(&self) -> usize;
}

#[derive(Debug)]
pub struct SmallAlloc {
    cursor: AtomicUsize,
    _cap: AtomicUsize,
    ptr: Cell<*mut u8>,
    l: Arc<std::sync::Mutex<()>>,
}

unsafe impl Send for SmallAlloc {}

unsafe impl Sync for SmallAlloc {}

impl Allocate for SmallAlloc {
    /// Alloc 8-byte aligned memory.
    fn alloc(&self, size: usize) -> usize {
        // Leave enough padding for alignment.
        let size = (size + PtrAlign) & !PtrAlign;
        let offset = self.cursor.fetch_add(size, Ordering::SeqCst);
        if offset + size > self.cap() {
            #[cfg(test)]
            crate::test_util::push_log_by_filename("aaaa", b"shift");
        }
        offset
    }

    fn size(&self) -> usize {
        todo!()
    }

    unsafe fn get_mut<T>(&self, offset: usize) -> *mut T {
        if offset == 0 {
            return ptr::null_mut();
        }
        return self.ptr.get().add(offset) as _;
    }

    fn offset<T>(&self, ptr: *const T) -> usize {
        let ptr_addr = ptr as usize;
        let base_addr = self.ptr.get() as usize;
        if ptr_addr > base_addr && ptr_addr < base_addr + self.cap() {
            ptr_addr - base_addr
        } else {
            0
        }
    }

    fn len(&self) -> usize {
        self.cursor.load(Ordering::SeqCst)
    }

    fn cap(&self) -> usize {
        self._cap.load(Ordering::SeqCst)
    }
}

impl SmallAlloc {
    pub fn new(cap: usize) -> SmallAlloc {
        let mut buf: Vec<u64> = Vec::with_capacity(cap / (PtrAlign + 1));
        let ptr = buf.as_mut_ptr() as *mut u8;
        let cap = buf.capacity() * (PtrAlign + 1);
        std::mem::forget(buf);
        SmallAlloc {
            cursor: AtomicUsize::new(PtrAlign + 1),
            _cap: AtomicUsize::new(cap),
            ptr: Cell::new(ptr),
            l: Arc::new(std::sync::Mutex::new(())),
        }
    }

    pub unsafe fn alloc_obj<T>(&self, size: usize) -> (*mut T, usize) {
        //assert!(self.cursor.load(Ordering::SeqCst) + size <= self.cap.get());
        let offset = self.alloc(size);
        let obj = self.get_mut::<T>(offset);
        (obj, offset)
    }

    pub fn reset(&self) {
        // self._cap
        // self.cursor.store(0, Ordering::Relaxed);
        //self.ptr.clear();
    }
}
