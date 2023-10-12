use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{size_of, ManuallyDrop};

use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::Node;
use std::ptr;

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
        let (pre, mid, _suf) = unsafe { ptr.align_to() };
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
        let (pre, mid, _suf) = unsafe { ptr.align_to() };
        assert!(pre.is_empty());
        &mid[0]
    }

    #[inline]
    pub fn get_mut(&self, start: usize) -> &mut T {
        let ptr = self.borrow_mut_slice(start, Self::size());
        let (pre, mid, _suf) = unsafe { ptr.align_to_mut() };
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

pub struct VecAllocate {
    cursor: Arc<AtomicUsize>,
    cap: Arc<AtomicUsize>,
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

    pub fn cap(&self) -> usize {
        self.cap.load(Ordering::Relaxed)
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

#[cfg(test)]
mod tests {
    use crate::skl::alloc::Chunk;
    use crate::{BlockBytes, Node, OnlyLayoutAllocate, SliceAllocate};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread::spawn;

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
}


#[cfg(test)]
mod testss{
    use std::{
        cell::Cell,
        mem, ptr,
        sync::atomic::{AtomicUsize, Ordering},
    };

    // Node 结构体
    #[derive(Debug)]
    #[repr(C)]
    pub struct Node {
        pub(crate) key_offset: u32,
        pub(crate) key_size: u16,
        pub(crate) height: u16,
        pub(crate) value: u64,
        pub(crate) tower: [u32; 4], // 假设最大高度为 4
    }

    // ArenaObject 枚举
    #[repr(align(8))]
    pub enum ArenaObject {
        Node {
            discriminant: u8,
            data: Node,
        },
        Data {
            discriminant: u8,
            data: Vec<u8>,
        },
    }

    const MAX_HEIGHT: usize = 4;

    pub struct Arena {
        len: AtomicUsize,
        cap: Cell<usize>,
        ptr: Cell<*mut u8>,
    }

    impl Arena {
        pub fn new(cap: usize) -> Arena {
            let mut buf: Vec<u64> = Vec::with_capacity(cap / 8);
            let ptr = buf.as_mut_ptr() as *mut u8;
            let cap = buf.capacity() * 8;
            mem::forget(buf);
            Arena {
                len: AtomicUsize::new(8),
                cap: Cell::new(cap),
                ptr: Cell::new(ptr),
            }
        }

        pub fn alloc_object(&self, object: ArenaObject) -> Option<usize> {
            let size = std::mem::size_of_val(&object);
            let offset = self.len.fetch_add(size, Ordering::SeqCst);
            // let size = std::mem::size_of::<ArenaObject>();
            println!("size: {}, {}", size, mem::size_of::<Node>());
            // let offset = self.len.fetch_add(size, Ordering::SeqCst);

            // 先检查是否需要扩展内存块，然后将 ArenaObject 复制到内存中
            if offset + size > self.cap.get() {
                self.grow();
            }

            unsafe {
                let dest = self.ptr.get().add(offset);
                std::ptr::copy_nonoverlapping(&object as *const ArenaObject as *const u8, dest, size);
            }

            Some(offset)
        }

        pub fn get_object(&self, offset: usize) -> Option<&ArenaObject> {
            if offset == 0 {
                None
            } else {
                let ptr = unsafe {self.ptr.get().add(offset) as *const ArenaObject};
                Some(unsafe { &*ptr })
            }
        }

        fn grow(&self) {
            // 实现内存块的扩展逻辑
            // 这里省略，可以根据需要实现内存扩展的逻辑
        }
    }

    #[test]
    fn main() {
        // 创建一个 Node 对象
        let node = Node {
            key_offset: 0,
            key_size: 10,
            height: 3,
            value: 42,
            tower: [0; MAX_HEIGHT],
        };

        // 创建一个 Vec<u8> 数据
        let data = vec![1, 2, 3, 4, 5];

        // 创建 Arena 实例
        let arena = Arena::new(1<< 12);

        // 分配 Node 对象并获取偏移量
        // 分配 Node 对象并获取偏移量
        let node_object = ArenaObject::Node {
            discriminant: 0, // 使用标签 0 表示 Node
            data: node,
        };
        let node_offset = arena.alloc_object(node_object).unwrap();

        // 分配 Vec<u8> 数据并获取偏移量
        let data_object = ArenaObject::Data {
            discriminant: 1, // 使用标签 1 表示 Data
            data,
        };
        let data_offset = arena.alloc_object(data_object).unwrap();

        // 获取存储在 Arena 中的 Node 对象
        if let Some(object) = arena.get_object(node_offset) {
            match object {
                ArenaObject::Node { data, .. } => {
                    println!("Stored Node: {:?}", data);
                }
                _ => println!("Invalid object type."),
            }
        }
    }

}
