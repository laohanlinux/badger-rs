mod alloc;
mod arena;
mod cursor;

pub mod small_allocate;

pub use alloc::{Allocate, BlockBytes, Chunk, SmartAllocate};
pub use arena::Arena;
pub use cursor::Cursor;

use crate::must_align;
use crate::y::ValueStruct;
use rand::prelude::*;
use std::cell::{Ref, RefCell};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::{cmp, ptr};

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;
const MAX_NODE_SIZE: usize = size_of::<Node>();

#[derive(Debug, Default)]
#[repr(C)]
pub struct Node {
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
    tower: [AtomicU32; MAX_HEIGHT],
}

impl Node {
    pub(crate) fn new(
        arena: &Arena<SmartAllocate>,
        key: &[u8],
        v: &ValueStruct,
        height: isize,
    ) -> Node {
        // The base level is already allocated in the node struct.
        let offset = arena.put_node(height);
        let mut node = Node::default();
        // 1: storage key
        node.key_offset = arena.put_key(key);
        node.key_size = key.len() as u16;
        // 2: storage value
        node.set_value(arena, v);

        node.height = height as u16;
        node
    }

    pub(crate) const fn size() -> usize {
        size_of::<Node>()
    }

    fn get_value_offset(&self) -> (u32, u16) {
        let value = self.value.load(Ordering::Acquire);
        Self::decode_value(value)
    }

    fn key(&self, arena: &Arena<SmartAllocate>) -> impl Chunk {
        must_align(self);
        arena.get_key(self.key_offset, self.key_size)
    }

    fn set_value(&self, arena: &Arena<SmartAllocate>, v: &ValueStruct) {
        let (value_offset, value_size) = arena.put_val(v);
        let value = Self::encode_value(value_offset, value_size as u16);
        self.value.store(value, Ordering::SeqCst);
    }

    fn get_next_offset(&self, h: usize) -> u32 {
        self.tower[h].load(Ordering::Acquire)
    }

    // FIXME Haha
    fn cas_next_offset(&self, h: usize, old: u32, val: u32) -> bool {
        old == self.tower[h].compare_and_swap(old, val, Ordering::SeqCst)
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
    arena: Arena<SmartAllocate>,
}

impl SkipList {
    pub fn new(arena: usize) -> Self {
        let arena = Arena::new(arena);
        let v = ValueStruct::default();
        let node = Node::new(&arena, b"", &v, MAX_HEIGHT as isize);
        SkipList {
            height: AtomicI32::from(1),
            head: RefCell::new(node),
            arena,
            _ref: AtomicI32::from(1),
        }
    }

    /// Increases the reference count
    pub fn incr_ref(&self) {
        self._ref.fetch_add(1, Ordering::AcqRel);
    }

    // Sub crease the reference count
    pub fn decr_ref(&self) {
        self._ref.fetch_sub(1, Ordering::AcqRel);
    }

    fn valid(&self) -> bool {
        todo!()
    }

    fn get_next(&self, nd: &Node, height: isize) -> Option<&Node> {
        self.arena
            .get_node(nd.get_next_offset(height as usize) as usize)
    }

    // Finds the node near to key.
    // If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
    // node.key <= key (if allowEqual=true).
    // If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
    // node.key >= key (if allowEqual=true).
    // Returns the node found. The bool returned is true if the node has key equal to given key.
    fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> (Option<&Node>, bool) {
        let mut x = unsafe { &*(self.head.as_ptr() as *const Node) };
        let mut level = self.get_height();
        loop {
            // Assume x.key < key
            let mut next = self.get_next(x, level);
            if next.is_none() {
                // x.key < key < END OF LIST
                if level > 0 {
                    // Can descend further to iterator closer to the end.
                    level -= 1;
                    continue;
                }
                // Level=0. Cannot descend further. Let's return something that makes sense.
                if !less {
                    return (None, false);
                }
                // Try to return x. Make sure it is not a head node.
                if ptr::eq(x, self.head.as_ptr()) {
                    return (None, false);
                }
                return (Some(x), false);
            }
            let next = next.unwrap();
            let next_key = next.key(&self.arena);
            match key.cmp(next_key.get_data()) {
                cmp::Ordering::Greater => {
                    // x.key < next.key < key. We can continue to move right.
                    x = next;
                }
                cmp::Ordering::Equal => {
                    // x.key < key == next.key.
                    if allow_equal {
                        return (Some(next), true);
                    }
                    if !less {
                        // We want >, so go to base level to grab the next bigger note.
                        return (self.get_next(next, 0), false);
                    }
                    // We want <. If not base level, we should go closer in the next level.
                    if level > 0 {
                        level -= 1;
                        continue;
                    }
                    // On base level. Return x.
                    if ptr::eq(x, self.head.as_ptr()) {
                        return (None, false);
                    }

                    return (Some(x), false);
                }
                // cmp < 0. In other words, x.key < key < next.
                cmp::Ordering::Less => {
                    if level > 0 {
                        level -= 1;
                        continue;
                    }

                    // At base level. Need to return something
                    if !less {
                        return (Some(next), false);
                    }
                    // Try to return x. Make sure it is not a head node.
                    if ptr::eq(x, self.head.as_ptr()) {
                        return (None, false);
                    }
                    return (Some(x), false);
                }
            }
        }
    }

    //  returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
    // The input "before" tells us where to start looking.
    // If we found a node with the same key, then we return outBefore = outAfter.
    // Otherwise, outBefore.key < key < outAfter.key.
    fn find_splice_for_level<'a>(
        &'a self,
        key: &'a [u8],
        mut before: &'a Node,
        level: isize,
    ) -> (&'a Node, Option<&'a Node>) {
        loop {
            // Assume before.key < key.
            let mut next = self.get_next(before, level);
            if next.is_none() {
                return (before, next);
            }
            let mut next = next.unwrap();
            let next_key = next.key(&self.arena);
            match key.cmp(next_key.get_data()) {
                cmp::Ordering::Equal => {
                    return (next, Some(next));
                }
                cmp::Ordering::Less => {
                    return (before, Some(next));
                }
                cmp::Ordering::Greater => {
                    before = next; // Keep moving right on this level.
                }
            }
        }
    }

    fn get_height(&self) -> isize {
        self.height.load(Ordering::Acquire) as isize
    }

    /// Inserts the key-value pair.
    /// FIXME: it bad, should be not use unsafe, but ....
    unsafe fn put(&self, key: &[u8], v: ValueStruct) {
        // Since we allow overwrite, we may not need to create a new node. We might not even need to
        // increase the height. Let's defer these actions.
        // let mut def_node = &mut Node::default();
        let list_height = self.get_height();
        let vec_node = |n| -> Box<Vec<Node>> {
            let mut v = Vec::with_capacity(n);
            for i in 0..n {
                v.push(Node::default());
            }
            Box::new(v)
        };
        let mut binding = vec_node((HEIGHT_INCREASE + 1) as usize);
        let mut prev = binding
            .as_mut_slice()
            .iter()
            .map(|node| node as *const Node)
            .collect::<Vec<_>>();
        {
            let mut head = self.head.borrow();
            prev[list_height as usize] = &*head;
        }

        let mut binding = vec_node((HEIGHT_INCREASE + 1) as usize);
        let mut next = binding
            .as_mut_slice()
            .iter()
            .map(|node| node as *const Node)
            .collect::<Vec<_>>();
        {
            next[list_height as usize] = std::ptr::null();
        }

        for i in (0..list_height as usize).rev() {
            // Use higher level to speed up for current level.
            let cur = &*prev[i + 1];
            let (_pre, _next) = self.find_splice_for_level(key, cur, i as isize);
            if _next.is_some() && ptr::eq(_pre, _next.unwrap()) {
                prev[i].as_ref().unwrap().set_value(&self.arena, &v);
                return;
            }
        }

        // We do need to create a new node.
        let height = Self::random_height();
        let x = Node::new(&self.arena, key, &v, height as isize);

        // Try to increase a new node.
        let mut list_height = self.get_height() as i32;
        while height > list_height as usize {
            if self
                .height
                .compare_and_swap(list_height, height as i32, Ordering::Acquire)
                == list_height
            {
                // Successfully increased SkipList.height
                break;
            } else {
                list_height = self.get_height() as i32;
            }
        }

        // We always insert from the base level and up. After you add a node in base level, we cannot
        // create a node in the level above because it would have discovered the node in the base level.
        for i in 0..height {
            loop {
                if prev[i as usize].is_null() {
                    assert!(i > 1); // This cannot happen in base level.
                                    // We haven't computed prev, next for this level because height exceeds old list_height.
                                    // For these levels, we expect the lists to be sparse, so we can just search from head.
                    let mut head = self.head.borrow_mut();
                    let head = &mut *head;
                    let (_pre, _next) = self.find_splice_for_level(key, head, i as isize);
                    prev[i] = _pre as *const Node;
                    next[i] = _next.unwrap() as *const Node;

                    // Someone adds the exact same key before we are able to do so. This can only happen on
                    // the base level. But we know we are not on the base level.
                    assert!(!ptr::eq(prev[i], next[i]));
                }

                let next_offset = self.arena.get_node_offset(next[i]);
                x.tower[i].store(next_offset as u32, Ordering::SeqCst);
                if prev[i].as_ref().unwrap().cas_next_offset(
                    i,
                    next_offset as u32,
                    self.arena.get_node_offset(&x) as u32,
                ) {
                    // Managed to insert x between prev[i] and next[i]. Go to the next level.
                    break;
                }

                // CAS failed. We need to recompute prev and next.
                // It is unlikely to be helpful to try to use a different level as we redo the search,
                // because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
                let (_pre, _next) =
                    self.find_splice_for_level(key, prev[i].as_ref().unwrap(), i as isize);
                prev[i] = _pre as *const Node;
                // FIXME: maybe nil pointer
                next[i] = _next.unwrap() as *const Node;
                if ptr::eq(prev[i], next[i]) {
                    assert_eq!(i, 0, "Equality can happen only on base level: {}", i);
                    prev[i].as_ref().unwrap().set_value(&self.arena, &v);
                    return;
                }
            }
        }
    }

    pub fn empty(&self) -> bool {
        unsafe { self.find_last().is_none() }
    }

    // Returns the last element. If head (empty list), we return nil, All the find functions
    // will NEVER return the head nodes.
    unsafe fn find_last(&self) -> Option<&Node> {
        let mut n = self.head.as_ptr() as *const Node;
        let mut level = self.get_height() - 1;
        loop {
            let next = self.get_next(&*n, level);
            if next.is_some() {
                n = unsafe { next.unwrap() as *const Node };
                continue;
            }
            if level == 0 {
                if ptr::eq(n, self.head.as_ptr()) {
                    return None;
                }
                return Some(&*n);
            }
            level -= 1;
        }
    }

    // gets the value associated with the key.
    // FIXME: maybe return Option<&ValueStruct>
    fn get(&self, key: &[u8]) -> Option<ValueStruct> {
        let (node, found) = self.find_near(key, false, true);
        if !found {
            return None;
        }
        let (value_offset, value_size) = node.unwrap().get_value_offset();
        Some(self.arena.get_val(value_offset, value_size))
    }

    /// Returns a SkipList cursor. You have to close() the cursor.
    // pub fn new_cursor(&self) -> Cursor<'_, Self> {
    //     self.incr_ref();
    //     Cursor {
    //         list: self,
    //         item: RefCell::new(None),
    //     }
    // }

    /// returns the size of the Skiplist in terms of how much memory is used within its internal arena.
    pub fn mem_size(&self) -> u32 {
        self.arena.size()
    }

    fn random_height() -> usize {
        let mut h = 1;
        while h < MAX_HEIGHT && random::<u32>() <= HEIGHT_INCREASE {
            h += 1;
        }
        0
    }
}

mod tests {
    use crate::skl::SkipList;

    const ARENA_SIZE: usize = 1 << 20;

    fn new_value(v: usize) -> String {
        format!("{:05}", v)
    }

    fn length(s: &SkipList) -> usize {
        let mut x = s.get_next(&s.head.borrow(), 0);
        let mut count = 0;
        while x.is_some() {
            count += 1;
            x = s.get_next(x.unwrap(), 0);
        }
        count
    }

    #[test]
    fn t_empty() {
        let key = b"aaa";
        let st = SkipList::new(1024);
        let v = st.get(key);
        assert!(v.is_none());

        for (_, less) in vec![(true, false)] {
            for (_, allow_equal) in vec![(true, false)] {
                let node = st.find_near(key, less, allow_equal);
                assert!(node.0.is_none());
                assert_eq!(node.1, false);
            }
        }
    }
}
