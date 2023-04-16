use crate::skl::node::Node;
use rand::random;
use serde::de::IntoDeserializer;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::{align_of, size_of, ManuallyDrop};

use atom_box::AtomBox;
use atomic::Atomic;
use either::Either;
use libc::{difftime, off_t};
use log::info;
use std::alloc::alloc;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{sleep, spawn};
use std::time::Duration;
use std::{ptr, thread};

pub trait Allocate: Send + Sync {
    type Block;
    #[inline]
    fn alloc(&self, start: usize, n: usize) -> Self::Block;
    #[inline]
    fn size(&self) -> usize;
    #[inline]
    fn used_count(&self) -> usize;
}

pub trait Chunk: Send + Sync {
    #[inline]
    fn get_data(&self) -> &[u8];
    #[inline]
    fn get_data_mut(&self) -> &mut [u8];
    #[inline]
    fn size(&self) -> usize;
}

#[derive(Debug)]
pub struct ArcBlockBytes {
    start: AtomicPtr<u8>,
    n: Arc<AtomicUsize>,
}

impl ArcBlockBytes {
    pub fn new_null() -> Self {
        Self {
            start: AtomicPtr::new(ptr::null_mut()),
            n: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn set(&self, ptr: *mut u8, n: usize) {
        self.start.store(ptr, Ordering::Relaxed);
        self.n.store(n, Ordering::Relaxed);
    }
}

impl Clone for ArcBlockBytes {
    fn clone(&self) -> Self {
        let ptr = self.start.load(Ordering::Relaxed);
        Self {
            start: AtomicPtr::new(ptr),
            n: Arc::new(AtomicUsize::new(self.n.load(Ordering::Relaxed))),
        }
    }
}

impl Chunk for ArcBlockBytes {
    #[inline]
    fn get_data(&self) -> &[u8] {
        unsafe {
            &*slice_from_raw_parts(
                self.start.load(Ordering::Relaxed),
                self.n.load(Ordering::Relaxed),
            )
        }
    }

    #[inline]
    fn get_data_mut(&self) -> &mut [u8] {
        unsafe {
            &mut *slice_from_raw_parts_mut(
                self.start.load(Ordering::Relaxed),
                self.n.load(Ordering::Relaxed),
            )
        }
    }

    #[inline]
    fn size(&self) -> usize {
        self.n.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct BlockBytes {
    start: AtomicPtr<u8>,
    n: usize,
}

impl BlockBytes {
    pub(crate) fn new(start: *mut u8, n: usize) -> Self {
        BlockBytes {
            start: AtomicPtr::new(start),
            n,
        }
    }
    #[inline]
    fn is_null(&self) -> bool {
        self.n == 0
    }
}

impl Chunk for BlockBytes {
    #[inline]
    fn get_data(&self) -> &[u8] {
        unsafe { &*slice_from_raw_parts(self.start.load(Ordering::Relaxed), self.n) }
    }

    #[inline]
    fn get_data_mut(&self) -> &mut [u8] {
        unsafe { &mut *slice_from_raw_parts_mut(self.start.load(Ordering::Relaxed), self.n) }
    }

    #[inline]
    fn size(&self) -> usize {
        self.n
    }
}

impl Clone for BlockBytes {
    fn clone(&self) -> Self {
        let ptr = self.start.load(Ordering::Relaxed);
        Self {
            start: AtomicPtr::new(ptr),
            n: self.n,
        }
    }
}

// The alloc is only supported on layout, and it only append
#[derive(Debug, Clone)]
pub struct OnlyLayoutAllocate<T> {
    cursor: Arc<AtomicUsize>,
    cap: Arc<AtomicUsize>,
    pub(crate) ptr: ManuallyDrop<Vec<u8>>,
    _data: PhantomData<T>,
}

unsafe impl<T> Send for OnlyLayoutAllocate<T> {}

unsafe impl<T> Sync for OnlyLayoutAllocate<T> {}

impl<T> OnlyLayoutAllocate<T> {
    pub(crate) fn size() -> usize {
        size_of::<T>()
    }

    pub(crate) fn len(&self) -> usize {
        self.cursor.load(Ordering::Relaxed)
    }

    pub fn new(n: usize) -> Self {
        OnlyLayoutAllocate {
            cursor: Arc::from(AtomicUsize::new(Self::size())),
            cap: Arc::from(AtomicUsize::new(n)),
            ptr: ManuallyDrop::new(vec![0u8; n]),
            _data: Default::default(),
        }
    }

    /// *alloc* a new &T.
    /// **Note** if more than len, it will be panic.
    pub fn alloc(&self, start: usize) -> &T {
        let end = self.cursor.fetch_add(Self::size(), Ordering::Acquire);
        assert!(end < self.cap.load(Ordering::Relaxed));
        let ptr = self.borrow_slice(start, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to() };
        assert!(pre.is_empty());
        &mid[0]
    }

    /// *alloc* a new &mut T
    /// **Note** if more than len, it will be panic.
    pub fn mut_alloc(&self, start: usize) -> &mut T {
        let end = self.cursor.fetch_add(Self::size(), Ordering::Relaxed);
        assert!(end < self.cap.load(Ordering::Relaxed));
        let ptr = self.borrow_mut_slice(start, Self::size());
        let (pre, mid, _) = unsafe { ptr.align_to_mut() };
        assert!(pre.is_empty());
        &mut mid[0]
    }

    pub fn alloc_offset(&self) -> (&T, usize) {
        let offset = self.cursor.fetch_add(Self::size(), Ordering::Relaxed);
        assert!(offset + Self::size() < self.cap.load(Ordering::Relaxed));
        let ptr = self.borrow_slice(offset, Self::size());
        let (pre, mid, _) = unsafe { ptr.align_to() };
        assert!(pre.is_empty());
        (&mid[0], offset)
    }

    #[inline]
    pub fn get(&self, start: usize) -> &T {
        let ptr = self.borrow_slice(start, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to() };
        assert!(pre.is_empty());
        &mid[0]
    }

    #[inline]
    pub fn get_mut(&self, start: usize) -> &mut T {
        let mut ptr = self.borrow_mut_slice(start, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to_mut() };
        assert!(pre.is_empty());
        &mut mid[0]
    }

    pub(crate) fn reset(&self) {
        self.cap.store(0, Ordering::Relaxed);
        self.cursor.store(0, Ordering::Relaxed);
    }

    #[inline]
    fn borrow_mut_slice(&self, start: usize, n: usize) -> &mut [u8] {
        let ptr = self.get_data_ptr() as *mut u8;
        unsafe { &mut *slice_from_raw_parts_mut(ptr.add(start), n) }
    }

    #[inline]
    fn borrow_slice(&self, start: usize, n: usize) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { &*slice_from_raw_parts(ptr.add(start), n) }
    }

    #[inline]
    pub(crate) fn get_data_mut_ptr(&self) -> *mut u8 {
        self.get_data_ptr() as *mut u8
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
}

impl<T> Drop for OnlyLayoutAllocate<T> {
    fn drop(&mut self) {
        self.cursor.store(0, Ordering::Relaxed);
        unsafe {
            ManuallyDrop::drop(&mut self.ptr);
        }
    }
}

// The alloc is only supported on layout, and it only append
#[derive(Debug)]
pub struct SliceAllocate {
    cursor: Arc<AtomicUsize>,
    cap: Arc<AtomicUsize>,
    pub(crate) ptr: ManuallyDrop<Vec<u8>>,
}

unsafe impl Send for SliceAllocate {}

impl SliceAllocate {
    pub fn len(&self) -> usize {
        self.cursor.load(Ordering::Relaxed)
    }

    pub(crate) fn new(n: usize) -> Self {
        SliceAllocate {
            cursor: Arc::from(AtomicUsize::new(1)),
            cap: Arc::from(AtomicUsize::new(n)),
            ptr: ManuallyDrop::new(vec![0u8; n]),
        }
    }

    pub(crate) fn get(&self, start: usize, size: usize) -> &[u8] {
        self.borrow_slice(start, size)
    }

    // Return the start locate offset
    pub(crate) fn alloc(&self, size: usize) -> &[u8] {
        let offset = self.cursor.fetch_add(size, Ordering::Relaxed);
        assert!(self.cursor.load(Ordering::Relaxed) < self.cap.load(Ordering::Relaxed));
        self.borrow_slice(offset, size)
    }

    fn alloc_mut(&self, size: usize) -> &mut [u8] {
        let offset = self.cursor.fetch_add(size, Ordering::Relaxed);
        assert!(self.cursor.load(Ordering::Relaxed) < self.cap.load(Ordering::Relaxed));
        self.borrow_mut_slice(offset, size)
    }

    pub fn append(&self, bytes: &[u8]) -> usize {
        let offset = self.cursor.fetch_add(bytes.len(), Ordering::Relaxed);
        assert!(self.cursor.load(Ordering::Relaxed) < self.cap.load(Ordering::Relaxed));
        let buffer = self.borrow_mut_slice(offset, bytes.len());
        buffer.copy_from_slice(bytes);
        offset
    }

    pub fn fill(&self, start: usize, bytes: &[u8]) -> usize {
        let buffer = self.borrow_mut_slice(start, bytes.len());
        buffer.copy_from_slice(bytes);
        start + buffer.len()
    }

    pub fn reset(&self) {
        self.cap.swap(0, Ordering::Relaxed);
        self.cursor.store(0, Ordering::Relaxed);
        //self.ptr.clear();
    }

    #[inline]
    fn borrow_mut_slice(&self, start: usize, n: usize) -> &mut [u8] {
        let ptr = self.get_data_mut_ptr();
        unsafe { &mut *slice_from_raw_parts_mut(ptr.add(start), n) }
    }

    #[inline]
    fn borrow_slice(&self, start: usize, n: usize) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { &*slice_from_raw_parts(ptr.add(start), n) }
    }

    #[inline]
    pub(crate) fn get_data_mut_ptr(&self) -> *mut u8 {
        self.get_data_ptr() as *mut u8
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
}

#[test]
fn t_onlylayoutalloc() {
    let mut alloc: OnlyLayoutAllocate<Node> = OnlyLayoutAllocate::new(1 << 10);
    for i in 0..10 {
        let key = alloc.mut_alloc(i * Node::size());
        key.value.fetch_add(1, Ordering::Relaxed);
    }

    for i in 0..10 {
        let key = alloc.get(i * Node::size());
        assert_eq!(key.value.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn t_onlylayoutalloc_currency() {
    let alloc: Arc<OnlyLayoutAllocate<Node>> = Arc::new(OnlyLayoutAllocate::new(1 << 20));
    let mut wait = vec![];
    for i in 0..100 {
        let alloc = alloc.clone();
        wait.push(spawn(move || {
            for i in 0..10 {
                let key = alloc.mut_alloc(i * Node::size());
                key.value.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for join in wait {
        join.join().unwrap();
    }
    for i in 0..10 {
        let key = alloc.get(i * Node::size());
        assert_eq!(key.value.load(Ordering::Relaxed), 100);
    }
}

#[test]
fn t_onlylayoutalloc_slice() {
    let mut alloc: SliceAllocate = SliceAllocate::new(1 << 10);
    for i in 0..10 {
        let key = alloc.alloc_mut(i * 10);
        key.fill(20);
    }
}

#[test]
fn t_block_bytes() {
    let mut buffer = vec![0u8; 1024];
    let block = BlockBytes::new(buffer.as_mut_ptr(), 10);
    {
        let data = block.get_data_mut();
        for datum in 0..data.len() {
            data[datum] = datum as u8;
        }
    }
    for datum in 0..block.size() {
        assert_eq!(buffer[datum], datum as u8);
    }
}
