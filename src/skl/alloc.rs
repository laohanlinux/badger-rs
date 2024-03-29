use crate::{cals_size_with_align};
use std::fmt::Debug;
use std::mem::{ManuallyDrop};
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const PtrAlign: usize = 7;

pub trait Allocate: Send + Sync {
    #[inline]
    fn alloc(&self, size: usize) -> usize;
    #[inline]
    fn alloc_rev(&self, size: usize) -> usize {
        todo!()
    }
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
pub struct DoubleAlloc {
    pub(crate) head: AtomicUsize,
    pub(crate) tail: AtomicUsize,
    ptr: ManuallyDrop<Vec<u8>>,
    _cap: usize,
}

unsafe impl Send for DoubleAlloc {}

impl Drop for DoubleAlloc {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.ptr);
        }
    }
}

impl Allocate for DoubleAlloc {
    fn alloc(&self, size: usize) -> usize {
        let free_count = self.free_count();
        // info!("{}", free_count);
        assert!(free_count > size, "less memory");
        let offset = self.head.fetch_add(size, Ordering::SeqCst);
        offset
    }

    fn alloc_rev(&self, size: usize) -> usize {
        let free_count = self.free_count();
        assert!(free_count > size, "less memory");
        let offset = self.tail.fetch_sub(size, Ordering::SeqCst);
        offset - size
    }

    fn size(&self) -> usize {
        todo!()
    }

    unsafe fn get_mut<T>(&self, offset: usize) -> *mut T {
        let ptr = self.ptr.as_ptr() as *mut u8;
        ptr.add(offset).cast::<T>()
    }

    fn offset<T>(&self, ptr: *const T) -> usize {
       let base_ptr = self.ptr.as_ptr() as usize;
        let offset_ptr = ptr as usize;
        offset_ptr - base_ptr
    }

    fn len(&self) -> usize {
        self.cap() - (self.tail.load(Ordering::SeqCst) - self.head.load(Ordering::SeqCst))
    }

    fn cap(&self) -> usize {
        self._cap
    }
}

impl DoubleAlloc {
    pub(crate) fn new(n: usize) -> DoubleAlloc {
        let n = cals_size_with_align(n, PtrAlign);
        assert_eq!(n % (PtrAlign + 1), 0);
        DoubleAlloc {
            head: AtomicUsize::new(PtrAlign + 1),
            tail: AtomicUsize::new(n),
            ptr: ManuallyDrop::new(vec![0u8; n]),
            _cap: n,
        }
    }

    fn free_count(&self) -> usize {
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);
        assert!(head < tail, "head({}) should be lt tail({})", head, tail);
        tail - head
    }
}

#[test]
fn t() {
    let size = (1 + 0) & !0;
    println!("{}, {}, {}", (1 + 0) & !0, (0 + 0) & !0, (3 + 0) & !0);
}
