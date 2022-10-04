use crate::skl::node::Node;
use rand::random;
use serde::de::IntoDeserializer;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::{align_of, size_of, ManuallyDrop};
use std::ptr;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

pub trait Allocate: Send + Sync {
    type Block;
    fn alloc(&self, start: usize, n: usize) -> Self::Block;
    fn size(&self) -> usize;
}

pub trait Chunk: Send + Sync {
    fn get_data(&self) -> &[u8];
    fn get_data_mut(&self) -> &mut [u8];
    fn size(&self) -> usize;
}

#[derive(Debug)]
#[repr(C)]
pub struct SmartAllocate {
    pub(crate) ptr: std::mem::ManuallyDrop<Vec<u8>>,
}

impl Allocate for SmartAllocate {
    type Block = impl Chunk;

    fn alloc(&self, start: usize, n: usize) -> Self::Block {
        let ptr = self.get_data_ptr();
        let block_ptr = unsafe { ptr.add(start) as *mut u8 };
        let block = BlockBytes::new(NonNull::new(block_ptr).unwrap(), n);
        block
    }

    fn size(&self) -> usize {
        self.ptr.len()
    }
}

impl SmartAllocate {
    pub(crate) fn new(m: std::mem::ManuallyDrop<Vec<u8>>) -> Self {
        println!("new a alloc memory, len: {}", m.len());
        SmartAllocate { ptr: m }
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
}

#[derive(Clone, Debug)]
#[repr(C)]
pub struct BlockBytes {
    start: NonNull<u8>,
    n: usize,
}

unsafe impl Send for BlockBytes {}

unsafe impl Sync for BlockBytes {}

impl BlockBytes {
    pub(crate) fn new(start: NonNull<u8>, n: usize) -> Self {
        BlockBytes { start, n }
    }
}

impl Chunk for BlockBytes {
    fn get_data(&self) -> &[u8] {
        unsafe { &*slice_from_raw_parts(self.start.as_ptr(), self.n) }
    }

    fn get_data_mut(&self) -> &mut [u8] {
        unsafe { &mut *slice_from_raw_parts_mut(self.start.as_ptr(), self.n) }
    }

    fn size(&self) -> usize {
        self.n
    }
}

// The alloc is only supported on layout, and it only append
#[derive(Debug, Clone)]
pub struct OnlyLayoutAllocate<T> {
    cursor: Arc<AtomicUsize>,
    len: Arc<AtomicUsize>,
    pub(crate) ptr: ManuallyDrop<Vec<u8>>,
    _data: PhantomData<T>,
}

unsafe impl<T> Send for OnlyLayoutAllocate<T> {}

unsafe impl<T> Sync for OnlyLayoutAllocate<T> {}

impl<T> OnlyLayoutAllocate<T> {
    fn size() -> usize {
        size_of::<T>()
    }

    pub fn new(n: usize) -> Self {
        OnlyLayoutAllocate {
            cursor: Arc::from(AtomicUsize::new(Self::size())),
            len: Arc::from(AtomicUsize::new(n)),
            ptr: ManuallyDrop::new(vec![0u8; n]),
            _data: Default::default(),
        }
    }

    /// *alloc* a new &T.
    /// **Note** if more than len, it will be panic.
    pub fn alloc(&self, start: usize) -> &T {
        let end = self.cursor.fetch_add(Self::size(), Ordering::Acquire);
        assert!(end < self.len.load(Ordering::Relaxed));
        let ptr = self.borrow_slice(start, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to() };
        assert!(pre.is_empty());
        &mid[0]
    }

    /// *alloc* a new &mut T
    /// **Note** if more than len, it will be panic.
    pub fn mut_alloc(&mut self, start: usize) -> &mut T {
        let end = self.cursor.fetch_add(Self::size(), Ordering::Relaxed);
        assert!(end < self.len.load(Ordering::Relaxed));
        let mut ptr = self.borrow_mut_slice(start, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to_mut() };
        assert!(pre.is_empty());
        &mut mid[0]
    }

    pub fn alloc_offset(&self) -> (&T, usize) {
        let offset = self.cursor.fetch_add(Self::size(), Ordering::Relaxed);
        assert!(offset + Self::size() < self.len.load(Ordering::Relaxed));
        let mut ptr = self.borrow_slice(offset, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to() };
        assert!(pre.is_empty());
        (&mid[0], offset)
    }

    pub fn alloc_mut_offset(&mut self) -> (&mut T, usize) {
        let offset = self.cursor.fetch_add(Self::size(), Ordering::Relaxed);
        assert!(offset + Self::size() < self.len.load(Ordering::Relaxed));
        let mut ptr = self.borrow_mut_slice(offset, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to_mut() };
        assert!(pre.is_empty());
        (&mut mid[0], offset)
    }

    #[inline]
    pub fn get(&self, start: usize) -> &T {
        let ptr = self.borrow_slice(start, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to() };
        assert!(pre.is_empty());
        &mid[0]
    }

    #[inline]
    pub fn get_mut(&mut self, start: usize) -> &mut T {
        let mut ptr = self.borrow_mut_slice(start, Self::size());
        let (pre, mid, suf) = unsafe { ptr.align_to_mut() };
        assert!(pre.is_empty());
        &mut mid[0]
    }

    pub(crate) fn reset(&mut self) {
        self.len.store(0, Ordering::Relaxed);
        self.cursor.store(0, Ordering::Relaxed);
        self.ptr.clear();
    }

    #[inline]
    fn borrow_mut_slice(&mut self, start: usize, n: usize) -> &mut [u8] {
        let ptr = self.get_data_mut_ptr();
        unsafe { &mut *slice_from_raw_parts_mut(ptr.add(start), n) }
    }

    #[inline]
    fn borrow_slice(&self, start: usize, n: usize) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { &*slice_from_raw_parts(ptr.add(start), n) }
    }

    #[inline]
    pub(crate) fn get_data_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_mut_ptr()
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
}

impl<T> Drop for OnlyLayoutAllocate<T> {
    fn drop(&mut self) {
        self.cursor.store(0, Ordering::Relaxed);
        self.cursor.store(0, Ordering::Relaxed);
        // TODO: free memory
        unsafe {
            // ManuallyDrop::into_inner(self.ptr);
        }
    }
}

// The alloc is only supported on layout, and it only append
#[derive(Debug)]
pub struct SliceAllocate {
    cursor: Arc<AtomicUsize>,
    len: Arc<AtomicUsize>,
    pub(crate) ptr: ManuallyDrop<Vec<u8>>,
}

unsafe impl Send for SliceAllocate {}

unsafe impl Sync for SliceAllocate {}

impl SliceAllocate {
    fn size(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub(crate) fn new(n: usize) -> Self {
        SliceAllocate {
            cursor: Arc::from(AtomicUsize::new(1)),
            len: Arc::from(AtomicUsize::new(n)),
            ptr: ManuallyDrop::new(vec![0u8; n]),
        }
    }

    pub(crate) fn get(&self, start: usize, size: usize) -> &[u8] {
        self.borrow_slice(start, size)
    }

    // fn get_mut(&mut self, start: usize, size: usize) -> &mut [u8] {
    //     self.borrow_mut_slice(start, size)
    // }

    // Return the start locate offset
    pub(crate) fn alloc(&self, size: usize) -> &[u8] {
        let offset = self.cursor.fetch_add(size, Ordering::Relaxed);
        assert!(self.cursor.load(Ordering::Relaxed) < self.len.load(Ordering::Relaxed));
        self.borrow_slice(offset, size)
    }

    fn alloc_mut(&mut self, size: usize) -> &mut [u8] {
        let offset = self.cursor.fetch_add(size, Ordering::Relaxed);
        assert!(self.cursor.load(Ordering::Relaxed) < self.len.load(Ordering::Relaxed));
        self.borrow_mut_slice(offset, size)
    }

    pub fn append(&mut self, bytes: &[u8]) -> usize {
        let offset = self.cursor.fetch_add(bytes.len(), Ordering::Relaxed);
        assert!(self.cursor.load(Ordering::Relaxed) < self.len.load(Ordering::Relaxed));
        let mut buffer = self.borrow_mut_slice(offset, bytes.len());
        buffer.copy_from_slice(bytes);
        offset
    }

    pub fn fill(&mut self, start: usize, bytes: &[u8]) -> usize {
        let mut buffer = self.borrow_mut_slice(start, bytes.len());
        buffer.copy_from_slice(bytes);
        start + buffer.len()
    }

    pub fn reset(&mut self) {
        self.len.swap(0, Ordering::Relaxed);
        self.cursor.store(0, Ordering::Relaxed);
        self.ptr.clear();
    }

    #[inline]
    fn borrow_mut_slice(&mut self, start: usize, n: usize) -> &mut [u8] {
        let ptr = self.get_data_mut_ptr();
        unsafe { &mut *slice_from_raw_parts_mut(ptr.add(start), n) }
    }

    #[inline]
    fn borrow_slice(&self, start: usize, n: usize) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { &*slice_from_raw_parts(ptr.add(start), n) }
    }

    #[inline]
    pub(crate) fn get_data_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_mut_ptr()
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
}

// impl Allocate for SliceAllocate {
//     type Block = ();
//
//     fn alloc(&self, start: usize, n: usize) -> Self::Block {
//         todo!()
//     }
//
//     fn size(&self) -> usize {
//         todo!()
//     }
// }

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
    let block = BlockBytes::new(NonNull::new(buffer.as_mut_ptr()).unwrap(), 10);
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

#[test]
fn t_clone() {

}