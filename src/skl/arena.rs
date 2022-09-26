// use crate::skl::{Node, OwnedNode, MAX_HEIGHT, MAX_NODE_SIZE};
use crate::skl::small_allocate::SmallAllocate;
use crate::skl::{Allocate, arena};
use crate::skl::{alloc::Chunk, Node, SmartAllocate};
use crate::y::ValueStruct;
use std::default;
use std::fmt::format;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, size_of};
use std::ptr::{addr_of, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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
        self.n.load(Ordering::Relaxed)
    }

    pub(crate) fn cap(&self) -> usize {
        self.slice.size()
    }

    // TODO: maybe use MaybeUint instead
    pub(crate) fn reset(&self) {
        println!("reset memory");
        self.n.store(0, Ordering::Relaxed);
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

    pub(crate) fn get_mut_node(&self, offset: usize) -> Option<impl Chunk> {
        if offset == 0 {
            return None;
        }
        Some(self.slice.alloc(offset, Node::size()))
    }

    // Returns start location
    pub(crate) fn put_key(&self, key: &[u8]) -> u32 {
        let start = self.n.fetch_add(key.len() as u32, Ordering::Relaxed) as usize;
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
        let ref node = Node::default();
        let sz = Node::size();
        let ptr = {
            node as *const Node as *const u8
        };
        let slice = slice_from_raw_parts(ptr, sz);
        let slice = unsafe { &*slice };
        self.put_key(slice)
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
    for arg in input.iter() {
        let key = format!("{:02}", arg.0);
        let offset = arena.put_key(key.as_bytes());
        assert_eq!(offset, arg.0);
    }
    for ref arg in input {
        let key = arena.get_key(arg.0, arg.1);
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
fn t_node() {
    let mut node = Node::default();
    node.height = 100;
    node.value.fetch_add(1, Ordering::Relaxed);
    let mut buffer = node.get_mut_slice();
    let mut node2 = Node::from_slice_mut(&mut buffer);
    assert_eq!(node.height, node2.height);
    node2.value.fetch_add(1, Ordering::Relaxed);

    assert_eq!(node.value.load(Ordering::Relaxed), node2.value.load(Ordering::Relaxed));
}

#[test]
fn t_node2() {
    let mut buffer = [0; Node::size()];
    let mut node2 = Node::from_slice_mut(&mut buffer);
    node2.value = AtomicU64::new(0);
    node2.value.fetch_add(1, Ordering::Relaxed);


    let mut node3 = Node::from_slice_mut(&mut buffer);
    node3.value.fetch_add(1, Ordering::Relaxed);
    assert_eq!(node3.value.load(Ordering::Relaxed), 2);
}

#[test]
fn t_node3() {
    let mut v = vec![0; 1<<10];
    for i in 0..v.len() {
        v[i] = 0;
    }
    println!("{:?}", v);
    let alloc = SmartAllocate::new(ManuallyDrop::new(v));
    // let arena = Arena { n: AtomicU32::new(1), slice: alloc };
    let mut block = alloc.alloc(1, Node::size());
    let mut node = Node::from_slice_mut(block.get_data_mut());
    node.key_size = 10;
    node.key_offset = 100;
    node.value = AtomicU64::new(0);
    node.value.fetch_add(1, Ordering::SeqCst);

    // let mut node2 = arena.get_node_mut(1).unwrap();
    // assert_eq!(node.value.load(Ordering::SeqCst), node.value.load(Ordering::SeqCst));
}

#[test]
fn t_arena_store_node() {
    let arena = Arena::new(1 << 20);
    let mut starts = vec![];
    for i in 0..5 {
        let start = arena.put_node(i);
        let mut node = arena.get_node_mut(start as usize).unwrap();
        node.height = i as u16;
        node.value.fetch_add(i as u64, Ordering::Relaxed);
        starts.push((i, start));
    }

    for (i, start) in starts {
        let node = arena.get_node_mut(start as usize).unwrap();
        let value = node.value.load(Ordering::Relaxed);
        assert_eq!(node.height, i as u16);
        assert_eq!(value, i as u64);
    }
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

    // let v = ValueStruct::new(vec![89, 102, 13], 1, 2, 100);
    // let node = Node::new(&arena, b"ABC", &v, 1);
    // println!("{:?}", arena.slice);
    //
    // {
    //     let got_key = arena.get_key(node.key_offset, node.key_size);
    //     assert_eq!(got_key.get_data(), b"ABC");
    // }
    //
    // {
    //     let (value_offset, value_sz) = node.get_value_offset();
    //     let got_value = arena.get_val(value_offset, value_sz);
    //     assert_eq!(v, got_value);
    // }
}