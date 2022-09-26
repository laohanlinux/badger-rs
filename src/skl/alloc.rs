use std::fmt::{Display, Formatter};
use rand::random;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, size_of};
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::thread::sleep;
use std::time::Duration;
use crate::skl::Node;

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
fn t_allocate() {
    use std::sync::Arc;
    use std::thread::spawn;

    let m = std::mem::ManuallyDrop::new(vec![0u8; 1024]);
    let alloc = Arc::new(SmartAllocate::new(m));
    for i in 0..100 {
        let alloc = Arc::clone(&alloc);
        spawn(move || {
            let chunk = alloc.alloc(0, 10);
            let idx = random::<usize>() % 10;
            chunk.get_data_mut()[idx] = random::<u8>();
        })
            .join()
            .unwrap();
    }
    sleep(Duration::from_millis(200));
    println!("{:?}", alloc.alloc(0, 10).get_data());
}

#[test]
fn t_allocate_node() {
    let alloc = SmartAllocate::new(ManuallyDrop::new(vec![0; 1 << 10]));
    {
        let block = alloc.alloc(0, 1);
        block.get_data_mut()[0] = 10;
    }
    println!("{:?}", alloc);
    let block = alloc.alloc(Node::size(), Node::size());
    let node = Node::from_slice_mut(block.get_data_mut());
    node.value.fetch_add(1, Ordering::Relaxed);
    //
    // let node = Node::from_slice_mut(block.get_data_mut());
    // assert_eq!(node.value.load(Ordering::Relaxed), 1);
}

#[test]
fn t_allocate_struct() {
    struct Node {
        key_sz: usize,
        key_offset: usize,
        value: AtomicPtr<u64>,
    }

    impl Node {
        fn size() -> usize {
            size_of::<Node>()
        }

        fn get_slice(&self) -> &[u8] {
            let ptr = self.get_ptr();
            unsafe {
                &*slice_from_raw_parts(ptr, Node::size())
            }
        }

        fn get_mut_slice(&self) -> &mut [u8] {
            let ptr = self.get_mut_ptr();
            unsafe { &mut *slice_from_raw_parts_mut(ptr, Node::size()) }
        }

        fn get_ptr(&self) -> *const u8 {
            self as *const Node as *const u8
        }

        fn get_mut_ptr(&self) -> *mut u8 {
            self as *const Node as *mut u8
        }

        #[inline]
        pub(crate) fn from_slice_mut(mut buffer: &mut [u8]) -> &mut Self {
            unsafe { &mut *(buffer.as_mut_ptr() as *mut Node) }
        }
    }

    let alloc = SmartAllocate::new(ManuallyDrop::new(vec![0; 1 << 20]));
    for i in 0..10 {
        let block = alloc.alloc(i * Node::size() + 1, Node::size());
        let mut node = Node::from_slice_mut(block.get_data_mut());
        node.key_sz = i;
        node.key_offset = i * 2;
        node.value.fetch_byte_add(1, Ordering::SeqCst);
    }
    for i in 0..10 {
        let block = alloc.alloc(i * Node::size() + 1, Node::size());
        let mut node = Node::from_slice_mut(block.get_data_mut());
        assert_eq!(node.key_sz, i);
        assert_eq!(node.key_offset, i * 2);
        // assert_eq!(node.value.load(Ordering::Relaxed), 1);
    }
}
