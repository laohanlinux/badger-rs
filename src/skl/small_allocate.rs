use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

pub trait Allocate {
    fn borrow_vec(&self, start: usize, n: usize) -> Slice;
    fn borrow_slice(&self, start: usize, n: usize) -> &[u8];
    fn borrow_mut_slice(&mut self, start: usize, n: usize) -> &mut [u8];
    fn fill_slice(&self, start: usize, n: usize, slice: &mut Slice);
}

#[derive(Debug)]
#[repr(C)]
pub struct SmallAllocate {
    pub(crate) ptr: PhantomData<u8>,
}

#[derive(Clone, Debug)]
pub struct Slice {
    start: *const u8,
    end: *const u8,
    n: usize,
}

unsafe impl Send for Slice {}

unsafe impl Sync for Slice {}

impl Slice {
    pub(crate) fn new(start: *const u8, n: usize) -> Self {
        Slice {
            n,
            start,
            end: unsafe { start.add(n) },
        }
    }

    pub(crate) fn get_data(&self) -> &[u8] {
        unsafe { &*slice_from_raw_parts(self.start, self.n) }
    }

    pub(crate) fn get_mut_data(&mut self) -> &mut [u8] {
        let ptr = self.start as *mut u8;
        unsafe { &mut *slice_from_raw_parts_mut(ptr, self.n) }
    }

    pub(crate) fn size(&self) -> usize {
        self.n
    }
}

impl Into<&[u8]> for Slice {
    fn into(self) -> &'static [u8] {
        unsafe { &*slice_from_raw_parts(self.start, self.n) }
    }
}

impl Allocate for SmallAllocate {
    fn borrow_vec(&self, start: usize, n: usize) -> Slice {
        let ptr = self.get_data_ptr();
        Slice {
            n,
            start: ptr,
            end: unsafe { ptr.add(start + n) },
        }
    }

    fn borrow_slice(&self, start: usize, n: usize) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { &*slice_from_raw_parts(ptr.add(start), n) }
    }

    fn borrow_mut_slice(&mut self, start: usize, n: usize) -> &mut [u8] {
        let ptr = self.get_data_mut_ptr();
        unsafe { &mut *slice_from_raw_parts_mut(ptr.add(start), n) }
    }

    fn fill_slice(&self, start: usize, n: usize, slice: &mut Slice) {
        let ptr = self.get_data_ptr();
        slice.n = n;
        slice.start = ptr;
        slice.end = unsafe { ptr.add(start + n) };
    }
}

impl SmallAllocate {
    #[inline]
    pub(crate) fn get_data_mut_ptr(&mut self) -> *mut u8 {
        &mut self.ptr as *mut PhantomData<u8> as *mut u8
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        &self.ptr as *const PhantomData<u8> as *const u8
    }

    #[inline]
    pub(crate) fn from_slice(buffer: Box<Vec<u8>>) -> &'static Self {
        unsafe { &*(buffer.as_ptr() as *const SmallAllocate) }
    }

    #[inline]
    pub(crate) fn from_slice_mut(mut buffer: Box<Vec<u8>>) -> &'static mut Self {
        unsafe { &mut *(buffer.as_mut_ptr() as *mut SmallAllocate) }
    }

    #[inline]
    pub(crate) fn from_slice_mut2(buffer: &mut [u8]) -> &'static mut Self {
        unsafe { &mut *(buffer.as_mut_ptr() as *mut SmallAllocate) }
    }
}

#[test]
fn t_small_allocate() {
    let mut allocate = SmallAllocate::from_slice(Box::new(vec![0u8; 1024]));
}
