mod arena;

use std::cell::RefCell;
use std::collections::HashMap;

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
    key_size: u32,

    // Height of the tower.
    height: u16,

    // parts of the value are encoded as a single uint64 so that it
    // can be atomically loaded and stored:
    //   value offset: uint32 (bits 0-31)
    //   value size  : uint16 (bits 32-47)
    value: u64,

    // Most nodes do not need to use the full height of the tower, since the
    // probability of each successive level decreases exponentially, Because
    // these elements are never accessed, the do not need to be allocated.
    // is deliberately truncated to not include unneeded tower elements.
    //
    // All accesses to elements should use CAS operations, with no need to lock.
    tower: [u8; MAX_HEIGHT],
}

impl Node {}

// Maps keys to value(in memory)
struct SkipList {
    height: i32,
    head: RefCell<Node>,
    _ref: i32,
}

#[derive(Clone)]
struct OwnedNode {
    buf: Vec<u8>,
}

impl OwnedNode {}

#[test]
fn it_works() {}
