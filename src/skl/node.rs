use crate::skl::arena::Arena;
use crate::skl::{MAX_HEIGHT, PtrAlign};
use crate::y::ValueStruct;
use std::mem::{align_of, size_of, size_of_val};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[derive(Debug)]
#[repr(C)]
pub struct Node {
    // A byte slice is 24 bytes. We are trying to save space here.
    // immutable. No need to lock to access key.
    pub(crate) key_offset: u32,
    // immutable. No need to lock to access key.
    pub(crate) key_size: u16,

    // Height of the tower.
    pub(crate) height: u16,

    // parts of the value are encoded as a single uint64 so that it
    // can be atomically loaded and stored:
    //   value offset: uint32 (bits 0-31)
    //   value size  : uint16 (bits 32-47)
    pub(crate) value: AtomicU64,

    // Most nodes do not need to use the full height of the tower, since the
    // probability of each successive level decreases exponentially, Because
    // these elements are never accessed, the do not need to be allocated.
    // is deliberately truncated to not include unneeded tower elements.
    //
    // All accesses to elements should use CAS operations, with no need to lock.
    pub(crate) tower: [AtomicU32; MAX_HEIGHT],
}

impl Default for Node {
    fn default() -> Self {
        const TOWER: AtomicU32 = AtomicU32::new(0);
        let mut node = Node {
            key_offset: 0,
            key_size: 0,
            height: 0,
            value: AtomicU64::new(0),
            tower: [TOWER; MAX_HEIGHT],
        };
        for i in 0..MAX_HEIGHT {
            node.tower[i] = AtomicU32::new(0);
        }
        node
    }
}

impl Node {
    pub(crate) fn new<'a>(
        arena: &'a mut Arena,
        key: &'a [u8],
        v: &'a ValueStruct,
        height: isize,
    ) -> &'a mut Node {
        let key_offset = arena.put_key(key);
        let (value_offset, value_size) = arena.put_val(v);
        // The base level is already allocated in the node struct.
        let offset = arena.put_node(height);
        let node = arena.get_mut_node(offset as usize).unwrap();
        // 1: storage key
        node.key_offset = key_offset;
        node.key_size = key.len() as u16;
        // 2: storage value
        node.value.store(
            Self::encode_value(value_offset, value_size),
            Ordering::Relaxed,
        );
        node.height = height as u16;
        node
    }

    pub(crate) const fn size() -> usize {
        size_of::<Node>()
    }

    pub(crate) const fn align_size() -> usize {
        (size_of::<Node>() + PtrAlign) & !PtrAlign
    }

    pub(crate) fn set_value(&self, arena: &Arena, v: &ValueStruct) {
        let (value_offset, value_size) = arena.put_val(v);
        let value = Self::encode_value(value_offset, value_size as u16);
        self.value.store(value, Ordering::Relaxed);
    }

    pub(crate) fn get_value_offset(&self) -> (u32, u16) {
        let value = self.value.load(Ordering::Acquire);
        Self::decode_value(value)
    }

    pub(crate) fn key<'a>(&'a self, arena: &'a Arena) -> &'a [u8] {
        arena.get_key(self.key_offset, self.key_size)
    }

    pub(crate) fn get_next_offset(&self, h: usize) -> u32 {
        self.tower[h].load(Ordering::Acquire)
    }

    pub(crate) fn cas_next_offset(&self, h: usize, old: u32, val: u32) -> bool {
        let ok = self.tower[h].compare_exchange(old, val, Ordering::Acquire, Ordering::Acquire);
        return ok.is_ok();
    }

    #[inline]
    fn decode_value(value: u64) -> (u32, u16) {
        let value_offset = value as u32;
        let value_size = (value >> 32) as u16;
        (value_offset, value_size)
    }

    #[inline]
    fn encode_value(value_offset: u32, value_size: u16) -> u64 {
        ((value_size as u64) << 32) | (value_offset) as u64
    }
}

