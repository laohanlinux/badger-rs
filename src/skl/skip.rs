use crate::skl::{Cursor, HEIGHT_INCREASE, MAX_HEIGHT};
use crate::BadgerErr;
use rand::random;
use std::sync::atomic::Ordering;
use std::{cmp, ptr, ptr::NonNull, sync::atomic::AtomicI32};

use crate::y::ValueStruct;

use super::{arena::Arena, node::Node};

pub struct SkipList {
    height: AtomicI32,
    head: NonNull<Node>,
    _ref: AtomicI32,
    pub(crate) arena: Arena,
}

unsafe impl Send for SkipList {}

unsafe impl Sync for SkipList {}

impl SkipList {
    pub fn new(arena_size: usize) -> Self {
        let mut arena = Arena::new(arena_size);
        let v = ValueStruct::default();
        let node = Node::new(&mut arena, "".as_bytes(), &v, MAX_HEIGHT as isize);
        Self {
            height: AtomicI32::new(1),
            head: NonNull::new(node).unwrap(),
            _ref: AtomicI32::new(1),
            arena: arena,
        }
    }

    /// Increases the reference count
    pub fn incr_ref(&self) {
        self._ref.fetch_add(1, Ordering::Relaxed);
    }
    // Sub crease the reference count
    pub fn decr_ref(&self) {
        let old = self._ref.fetch_sub(1, Ordering::SeqCst);
        if old > 1 {
            return;
        }
        self.arena.reset();
    }

    fn close(&mut self) -> Result<(), BadgerErr> {
        self.decr_ref();
        Ok(())
    }

    fn valid(&self) -> bool {
        self.arena.valid()
    }

    pub(crate) fn get_head(&self) -> &Node {
        let node = unsafe { self.head.as_ptr() as *const Node };
        unsafe { &*node }
    }

    fn get_head_mut(&self) -> &mut Node {
        let node = unsafe { self.head.as_ptr() as *mut Node };
        unsafe { &mut *node }
    }

    pub(crate) fn get_next(&self, nd: &Node, height: isize) -> Option<&Node> {
        self.arena
            .get_node(nd.get_next_offset(height as usize) as usize)
    }

    fn get_height(&self) -> isize {
        self.height.load(Ordering::Relaxed) as isize
    }

    // Finds the node near to key.
    // If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
    // node.key <= key (if allowEqual=true).
    // If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
    // node.key >= key (if allowEqual=true).
    // Returns the node found. The bool returned is true if the node has key equal to given key.
    pub(crate) fn find_near(
        &self,
        key: &[u8],
        less: bool,
        allow_equal: bool,
    ) -> (Option<&Node>, bool) {
        let mut x = self.get_head();
        let mut level = self.get_height() - 1;
        println!("start to hight: {}", level);
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
            match key.cmp(next_key) {
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

    // returns (outBefore, outAfter) with outBefore.key <= key <= outAfter.key.
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
            match key.cmp(next_key) {
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

    /// Inserts the key-value pair.
    /// FIXME: it bad, should be not use unsafe, but ....
    pub fn put(&mut self, key: &[u8], v: ValueStruct) {
        unsafe { self._put(key, v) }
    }

    unsafe fn _put(&mut self, key: &[u8], v: ValueStruct) {
        // Since we allow overwrite, we may not need to create a new node. We might not even need to
        // increase the height. Let's defer these actions.
        // let mut def_node = &mut Node::default();
        let list_height = self.get_height();
        let mut prev = [ptr::null::<Node>(); MAX_HEIGHT + 1].to_vec();
        prev[list_height as usize] = unsafe { self.get_head() as *const Node };
        let mut next = [ptr::null::<Node>(); MAX_HEIGHT + 1].to_vec();
        next[list_height as usize] = std::ptr::null();
        for i in (0..list_height as usize).rev() {
            // Use higher level to speed up for current level.
            let cur = unsafe { &*prev[i + 1] };
            let (_pre, _next) = self.find_splice_for_level(key, cur, i as isize);
            if _next.is_some() && ptr::eq(_pre, _next.unwrap()) {
                // TODO
                // prev[i].as_ref().unwrap().set_value(&self.arena, &v);
                return;
            }
            prev[i] = unsafe { _pre as *const Node };
            if _next.is_some() {
                next[i] = unsafe { _next.unwrap() as *const Node };
            }
        }

        // We do need to create a new node.
        let height = Self::random_height();
        let mut arena = self.arena.copy();
        let x = Node::new(arena.as_mut(), key, &v, height as isize);

        // Try to increase a new node.
        let mut list_height = self.get_height() as i32;
        while height > list_height as usize {
            if self
                .height
                .compare_and_swap(list_height, height as i32, Ordering::SeqCst)
                == list_height
            {
                // Successfully increased SkipList.height
                break;
            } else {
                list_height = self.get_height() as i32;
            }
            println!("try again");
        }

        // We always insert from the base level and up. After you add a node in base level, we cannot
        // create a node in the level above because it would have discovered the node in the base level.
        for i in 0..height {
            loop {
                if prev[i as usize].is_null() {
                    assert!(i > 1); // This cannot happen in base level.
                                    // We haven't computed prev, next for this level because height exceeds old list_height.
                                    // For these levels, we expect the lists to be sparse, so we can just search from head.
                    let mut head = self.get_head_mut();
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
                    self.arena.get_node_offset(unsafe { x as *const Node }) as u32,
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
                    // TODO
                    // prev[i].as_ref().unwrap().set_value(&self.arena, &v);
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
    pub unsafe fn find_last(&self) -> Option<&Node> {
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
        println!("find a key: {:?}", key);
        let (value_offset, value_size) = node.unwrap().get_value_offset();
        Some(self.arena.get_val(value_offset, value_size))
    }

    /// Returns a SkipList cursor. You have to close() the cursor.
    pub fn new_cursor(&self) -> Cursor<'_> {
        self.incr_ref();
        Cursor::new(self)
    }

    /// returns the size of the SkipList in terms of how much memory is used within its internal arena.
    pub fn mem_size(&self) -> u32 {
        self.arena.size()
    }

    fn random_height() -> usize {
        let mut h = 1;
        while h < MAX_HEIGHT && random::<u32>() <= HEIGHT_INCREASE {
            h += 1;
        }
        h
    }
}

#[test]
fn t() {}
