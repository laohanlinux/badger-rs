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
        // Don't store data at position 0 in order to reverse offset = 0 as a kind
        // of nil pointer
        Arena {
            n: AtomicU32::new(1),
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
        let ptr = block.get_data().as_ptr();
        unsafe { Some(&*(*ptr as *const Node)) }
    }

    pub(crate) fn get_node_mut(&self, offset: usize) -> Option<&mut Node> {
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
        assert!((start + key.len()) <= self.slice.size(), "start:{}, key:{},  {} <= {}", start, key.len(), start + key.len(), self.slice.size());
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
        let buf: Vec<u8> = v.into();
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
        let buffer = block.get_data_mut();
        ValueStruct::from(buffer)
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
    let arena = Arena::new(11);
    let input = vec![(1, 2), (3, 2), (5, 2), (7, 2), (9, 2)];
    for  arg in input.iter() {
        let key = format!("{:02}", arg.0);
        let offset = arena.put_key(key.as_bytes());
        assert_eq!(offset, arg.0);
    }
    for ref arg in input {
        let key= arena.get_key(arg.0, arg.1);
        let key = key.get_data();
        assert_eq!(key, format!("{:02}", arg.0).as_bytes());
    }
}

#[test]
fn t_arena_value() {
    let arena = Arena::new(1024);
    let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let value = ValueStruct {
        meta: 1,
        user_meta: 1,
        cas_counter: 2,
        value_sz: v.len() as u32,
        value: v,
    };
    let (start, n) = arena.put_val(&value);
    let load_value = arena.get_val(start, n);
    assert_eq!(value, load_value);
}

#[test]
fn t_arena_node() {
    let arena = Arena::new(1024);
    let v = ValueStruct::new(vec![89, 102, 13], 1, 2, 100);
    let node = Node::new(&arena, b"ABC", &v, 1);
    println!("{:?}", arena.slice);

    {
        let got_key = arena.get_key(node.key_offset, node.key_size);
        assert_eq!(got_key.get_data(), b"ABC");
    }

    {
        let (value_offset, value_sz) = node.get_value_offset();
        let got_value = arena.get_val(value_offset, value_sz);
        assert_eq!(v, got_value);
    }
}