use crate::skl::alloc::{OnlyLayoutAllocate, SliceAllocate};
use crate::skl::node::Node;
use crate::y::ValueStruct;
use std::mem::{size_of, ManuallyDrop};
use std::ptr::{addr_of, slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
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
pub struct Arena {
    slice: SliceAllocate,
    node_alloc: OnlyLayoutAllocate<Node>,
}

impl Arena {
    pub(crate) fn new(n: usize) -> Self {
        assert!(n > 0);
        let slice_alloc = SliceAllocate::new(n);
        let node_alloc = OnlyLayoutAllocate::new(n);
        // Don't store data at position 0 in order to reverse offset = 0 as a kind
        // of nil pointer
        Self {
            slice: slice_alloc,
            node_alloc,
        }
    }

    pub(crate) fn size(&self) -> u32 {
        (self.slice.len() + self.node_alloc.len()) as u32
    }

    pub(crate) fn cap(&self) -> u32 {
        self.slice.cap() as u32
    }

    // TODO: maybe use MaybeUint instead
    pub(crate) fn reset(&self) {
        self.slice.reset();
        self.node_alloc.reset();
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
        Some(self.node_alloc.get(offset))
    }

    pub(crate) fn get_mut_node(&self, offset: usize) -> Option<&mut Node> {
        if offset == 0 {
            return None;
        }
        Some(self.node_alloc.get_mut(offset))
    }

    // Returns start location
    pub(crate) fn put_key(&self, key: &[u8]) -> u32 {
        self.slice.append(key) as u32
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
    pub(crate) fn get_key(&self, offset: u32, size: u16) -> &[u8] {
        self.slice.get(offset as usize, size as usize)
    }

    // Returns byte slice at offset. The given size should be just the value
    // size and should NOT include the meta bytes.
    pub(crate) fn get_val(&self, offset: u32, size: u16) -> ValueStruct {
        let buffer = self.slice.get(offset as usize, size as usize);
        ValueStruct::from(buffer)
    }

    // Return byte slice at offset.
    // FIXME:
    pub(crate) fn put_node(&self, height: isize) -> u32 {
        let (_, offset) = self.node_alloc.alloc_offset();
        offset as u32
    }

    // Returns the offset of `node` in the arena. If the `node` pointer is
    // nil, then the zero offset is returned.
    pub(crate) fn get_node_offset(&self, node: *const Node) -> usize {
        if node.is_null() {
            return 0;
        }
        let node = node as *const u8;
        let ptr = self.node_alloc.ptr.as_ptr();
        let offset = unsafe { node.offset_from(ptr) };
        offset as usize
    }

    pub(crate) fn copy(&self) -> NonNull<Self> {
        let ptr = self as *const Self as *mut Self;
        NonNull::new(ptr).unwrap()
    }
}

#[test]
fn t_arena_key() {
    let arena = Arena::new(1 << 20);
    let keys = vec![vec![1, 2, 3], vec![4, 5, 6, 7, 90]];
    let mut got = vec![];
    for key in keys.iter() {
        got.push(arena.put_key(key));
    }
    for (i, offset) in got.iter().enumerate() {
        let key = arena.get_key(*offset, keys[i].len() as u16);
        assert_eq!(key, keys[i]);
    }
}

#[test]
fn t_arena_value() {
    let arena = Arena::new(1 << 20);
    let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let value = ValueStruct {
        meta: 1,
        user_meta: 1,
        cas_counter: 2,
        value: v,
    };
    let (start, n) = arena.put_val(&value);
    let load_value = arena.get_val(start, n);
    assert_eq!(value, load_value);
}

#[test]
fn t_arena_store_node() {
    let arena = Arena::new(1 << 20);
    let mut starts = vec![];
    for i in 0..5 {
        let start = arena.put_node(i);
        let node = arena.get_mut_node(start as usize).unwrap();
        node.height = i as u16;
        node.value.fetch_add(i as u64, Ordering::Relaxed);
        starts.push((i, start));
    }

    for (i, start) in starts {
        let node = arena.get_mut_node(start as usize).unwrap();
        let value = node.value.load(Ordering::Relaxed);
        assert_eq!(node.height, i as u16);
        assert_eq!(value, i as u64);
    }

    let second_node = arena.get_node(Node::size()).unwrap();
    let offset = arena.get_node_offset(second_node);
    assert_eq!(offset, Node::size());
}

#[test]
fn t_arena_currency() {
    let arena = Arc::new(Arena::new(1 << 20));
    let mut waits = vec![];
    for _i in 0..100 {
        let arena = Arc::clone(&arena);
        waits.push(spawn(move || arena.put_key(b"abc")));
    }

    let mut offsets = waits
        .into_iter()
        .map(|join| join.join().unwrap())
        .collect::<Vec<_>>();
    offsets.sort();
    println!("offsets: {:?}", offsets);
}
