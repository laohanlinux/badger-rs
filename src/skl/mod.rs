mod arena;
mod parallel_buffer;

use crate::skl::arena::Arena;
use crate::y::ValueStruct;
use rand::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

// pub const MAX_NODE_SIZE =

#[derive(Debug)]
#[repr(C)]
pub(crate) struct Node {
    // A byte slice is 24 bytes. We are trying to save space here.
    // immutable. No need to lock to access key.
    key_offset: u32,
    // immutable. No need to lock to access key.
    key_size: u16,

    // Height of the tower.
    height: u16,

    // parts of the value are encoded as a single uint64 so that it
    // can be atomically loaded and stored:
    //   value offset: uint32 (bits 0-31)
    //   value size  : uint16 (bits 32-47)
    value: AtomicU64,

    // Most nodes do not need to use the full height of the tower, since the
    // probability of each successive level decreases exponentially, Because
    // these elements are never accessed, the do not need to be allocated.
    // is deliberately truncated to not include unneeded tower elements.
    //
    // All accesses to elements should use CAS operations, with no need to lock.
    tower: [u8; MAX_HEIGHT],
}

impl Node {
    fn get_value_offset(&self) -> (u32, u16) {
        let value = self.value.load(Ordering::Acquire);
        Self::decode_value(value)
    }

    fn get_key<'a>(&'a self, arena: &'a Arena) -> &[u8] {
        arena.get_key(self.key_offset, self.key_size)
    }

    fn set_value(&self, arena: &mut Arena, v: ValueStruct) {
        let value_size = v.value.len() as u16;
        let value_offset = arena.put_value(v);
        let value = Self::encode_value(value_offset, value_size);
        self.value.store(value, Ordering::Relaxed);
    }

    fn get_next_offset(&self, h: usize) -> u32 {
        todo!()
    }

    fn cas_next_offset(&self, h: usize, old: u32, new: u32) -> bool {
        todo!()
    }

    fn decode_value(value: u64) -> (u32, u16) {
        let value_offset = value as u32;
        let value_size = (value >> 32) as u16;
        (value_offset, value_size)
    }

    fn encode_value(value_offset: u32, value_size: u16) -> u64 {
        ((value_size as u64) << 32) | (value_offset) as u64
    }
}

// Maps keys to value(in memory)
pub struct SkipList {
    height: AtomicI32,
    head: RefCell<Node>,
    _ref: AtomicI32,
    arena: Arena,
}

impl SkipList {
    /// Increases the reference count
    pub fn incr_ref(&self) {
        self._ref.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
struct OwnedNode {
    buf: Vec<u8>,
}

impl OwnedNode {}

fn random_height() -> usize {
    let mut h = 1;
    while h < MAX_HEIGHT && random::<u32>() <= HEIGHT_INCREASE {
        h += 1;
    }
    0
}

#[test]
fn value_decode() {
    let value_offset = 8713;
    let value_size = 184;
    let value = Node::encode_value(value_offset, value_size);
    let (got_value_offset, got_value_size) = Node::decode_value(value);
    assert_eq!(value_offset, got_value_offset);
    assert_eq!(value_size, got_value_size);
}
