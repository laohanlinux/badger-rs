// use crate::skl::{Node, OwnedNode, MAX_HEIGHT, MAX_NODE_SIZE};
use crate::skl::small_allocate::SmallAllocate;
use crate::skl::Allocate;
use crate::skl::{alloc::Chunk, Node, SmartAllocate};
use crate::y::ValueStruct;
use std::default;
use std::fmt::format;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{addr_of, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::Duration;

const OFFSET_SIZE: usize = size_of::<u32>();
// FIXME: i don't know
const PTR_ALIGN: usize = 7;

/// `Arena` should be lock-free.
#[derive(Debug)]
pub struct Arena<T: Allocate> {
    n: AtomicU32,
    slice: T,
}

unsafe impl Send for Arena<SmartAllocate> {}

unsafe impl Sync for Arena<SmartAllocate> {}

impl Arena<SmartAllocate> {
    pub(crate) fn new(n: usize) -> Arena<SmartAllocate> {
        assert!(n > 0);
        let m = std::mem::ManuallyDrop::new(vec![0u8; n]);
        let alloc = SmartAllocate::new(m);
        Arena {
            n: AtomicU32::new(0),
            slice: alloc,
        }
    }

    pub(crate) fn size(&self) -> u32 {
        self.n.load(Ordering::Acquire)
    }

    pub(crate) fn cap(&self) -> usize {
        self.slice.size()
    }

    // TODO: maybe use MaybeUint instead
    pub(crate) fn reset(&self) {
        println!("reset memory");
        self.n.store(0, Ordering::SeqCst);
        std::mem::drop(&self.slice.ptr);
    }

    pub(crate) fn valid(&self) -> bool {
        !self.slice.ptr.is_empty()
    }

    // Returns a pointer to the node located at offset. If the offset is
    // zero, then the null node pointer is returned.
    pub(crate) fn get_node(&self, offset: usize) -> Option<&Node> {
        if offset == 0 {
            return None;
        }
        let node_sz = Node::size();
        let block = self.slice.alloc(offset, node_sz);
        assert!((offset + node_sz) <= block.size());
        let ptr = block.get_data().as_ptr();
        unsafe { Some(&*(*ptr as *const Node)) }
    }

    pub(crate) fn get_mut_node(&self, offset: usize) -> Option<&mut Node> {
        if offset == 0 {
            return None;
        }
        let node_sz = Node::size();
        let block = self.slice.alloc(offset, node_sz);
        let ptr = block.get_data_mut().as_mut_ptr();
        unsafe { Some(&mut *(ptr as *mut Node)) }
    }

    // Returns start location
    pub(crate) fn put_key(&self, key: &[u8]) -> u32 {
        let start = self.n.fetch_add(key.len() as u32, Ordering::SeqCst) as usize;
        assert!((start + key.len()) <= self.slice.size());
        let block = self.slice.alloc(start, key.len());
        let block = block.get_data_mut();
        block.copy_from_slice(key);
        start as u32
    }

    // Put will *copy* val into arena. To make better use of this, reuse your input
    // val buffer. Returns an offset into buf. User is responsible for remembering
    // size of val. We could also store this size inside arena but the encoding and
    // decoding will incur some overhead.
    pub(crate) fn put_val(&self, v: &ValueStruct) -> (u32, u16) {
        let buf: Vec<u8> = v.to_vec();
        let offset = self.put_key(buf.as_slice());
        (offset, buf.len() as u16)
    }

    // Returns byte slice at offset.
    pub(crate) fn get_key(&self, offset: u32, size: u16) -> impl Chunk {
        let block = self.slice.alloc(offset as usize, size as usize);
        block
    }

    // Returns byte slice at offset. The given size should be just the value
    // size and should NOT include the meta bytes.
    pub(crate) fn get_val(&self, offset: u32, size: u16) -> ValueStruct {
        let block = self.slice.alloc(offset as usize, size as usize);
        block.get_data().into()
    }

    // Return byte slice at offset.
    // FIXME:
    pub(crate) fn put_node(&self, height: isize) -> u32 {
        let offset: usize = Node::size();
        // *Note*: the memory set 0 value.
        let key = vec![0u8; offset];
        self.put_key(&key)
    }

    // Returns the offset of `node` in the arena. If the `node` pointer is
    // nil, then the zero offset is returned.
    pub(crate) fn get_node_offset(&self, node: *const Node) -> usize {
        if node.is_null() {
            return 0;
        }
        let node = node as *const u8;
        let block = self.slice.alloc(0, 0);
        unsafe { node.offset_from(block.get_data().as_ptr()) as usize }
    }
}

#[test]
fn t_arena_key() {
    let arena = Arena::new(10);
    let mut starts = vec![];
    for i in (0..arena.cap()).step_by(2) {
        let key = format!("{:02}", i);
        starts.push(arena.put_key(key.as_bytes()));
    }

    for i in starts {
        let key = arena.get_key(i, 2);
        let key = key.get_data();
        assert_eq!(key, format!("{:02}", i).as_bytes());
    }
}

#[test]
fn t_arena_value() {
    let arena = Arena::new(1024);
    let value = ValueStruct {
        meta: 1,
        user_meta: 1,
        cas_counter: 2,
        value: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    };
    let (start, n) = arena.put_val(&value);
    let load_value = arena.get_val(start, n);
    assert_eq!(value, load_value);
}
