use crate::skl::{Cursor, HEIGHT_INCREASE, MAX_HEIGHT};
use crate::table::iterator::IteratorItem;
use crate::Xiterator;
use atom_box::AtomBox;
use log::{debug, info};
use rand::random;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{write, Display, Formatter};
use std::ops::Deref;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::{cmp, ptr, ptr::NonNull, sync::atomic::AtomicI32};
use tracing::field::debug;

use crate::y::ValueStruct;

use super::{arena::Arena, node::Node};

/// SkipList
pub struct SkipList {
    height: Arc<AtomicI32>,
    head: AtomicPtr<Node>,
    _ref: Arc<AtomicI32>,
    pub(crate) arena: Arc<Arena>,
}

impl Clone for SkipList {
    fn clone(&self) -> Self {
        let node = self.head.load(Ordering::Relaxed);
        SkipList {
            height: self.height.clone(),
            head: AtomicPtr::new(node),
            _ref: self._ref.clone(),
            arena: self.arena.clone(),
        }
    }
}

impl SkipList {
    pub fn new(arena_size: usize) -> Self {
        let mut arena = Arena::new(arena_size);
        let v = ValueStruct::default();
        let node = Node::new(&mut arena, "".as_bytes(), &v, MAX_HEIGHT as isize);
        Self {
            height: Arc::new(AtomicI32::new(1)),
            head: AtomicPtr::new(node),
            _ref: Arc::new(AtomicI32::new(1)),
            arena: Arc::new(arena),
        }
    }

    /// Increases the reference count
    pub fn incr_ref(&self) {
        self._ref.fetch_add(1, Ordering::Relaxed);
    }

    // Sub crease the reference count, deallocating the skiplist when done using it
    // TODO
    pub fn decr_ref(&self) {
        self._ref.fetch_sub(1, Ordering::Relaxed);
    }

    fn valid(&self) -> bool {
        self.arena_ref().valid()
    }

    pub(crate) fn arena_ref(&self) -> &Arena {
        &self.arena
    }

    pub(crate) fn arena_mut_ref(&self) -> &Arena {
        &self.arena
    }

    pub(crate) fn get_head(&self) -> &Node {
        unsafe { &*(self.head.load(Ordering::Relaxed) as *const Node) }
    }

    fn get_head_mut(&self) -> &mut Node {
        let node = unsafe { self.head.load(Ordering::Relaxed) as *mut Node };
        unsafe { &mut *node }
    }

    pub(crate) fn get_next(&self, nd: &Node, height: isize) -> Option<&Node> {
        self.arena_ref()
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
                if ptr::eq(x, self.head.load(Ordering::Relaxed)) {
                    return (None, false);
                }
                return (Some(x), false);
            }
            let next = next.unwrap();
            let next_key = next.key(self.arena_ref());
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
                    if ptr::eq(x, self.get_head()) {
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
                    if ptr::eq(x, self.get_head()) {
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
            let next = self.get_next(before, level);
            if next.is_none() {
                return (before, next);
            }
            let next = next.unwrap();
            let next_key = next.key(self.arena_ref());
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
    pub fn put(&self, key: &[u8], v: ValueStruct) {
        unsafe { self._put(key, v) }
    }

    unsafe fn _put(&self, key: &[u8], v: ValueStruct) {
        // Since we allow overwrite, we may not need to create a new node. We might not even need to
        // increase the height. Let's defer these actions.
        // let mut def_node = &mut Node::default();
        let list_height = self.get_height();
        let mut prev = [ptr::null::<Node>(); MAX_HEIGHT + 1].to_vec();
        prev[list_height as usize] = self.get_head();
        let mut next = [ptr::null::<Node>(); MAX_HEIGHT + 1].to_vec();
        next[list_height as usize] = std::ptr::null();
        for i in (0..list_height as usize).rev() {
            // Use higher level to speed up for current level.
            let cur = unsafe { &*prev[i + 1] };
            let (_pre, _next) = self.find_splice_for_level(key, cur, i as isize);
            prev[i] = _pre;
            if _next.is_some() && ptr::eq(_pre, _next.unwrap()) {
                let mut arena = self.arena_ref().copy().as_mut();
                prev[i].as_ref().unwrap().set_value(&arena, &v);
                return;
            }
            if _next.is_some() {
                next[i] = _next.unwrap() as *const Node;
            }
        }

        // We do need to create a new node.
        let height = Self::random_height();
        let mut arena = self.arena_ref().copy();
        let x = Node::new(arena.as_mut(), key, &v, height as isize);

        // Try to increase a new node. linked pre-->x-->next
        let mut list_height = self.get_height() as i32;
        while height > list_height as usize {
            if self
                .height
                .compare_exchange_weak(
                    list_height,
                    height as i32,
                    Ordering::Acquire,
                    Ordering::Acquire,
                )
                .is_ok()
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
                    // This cannot happen in base level.
                    // We haven't computed prev, next for this level because height exceeds old list_height.
                    // For these levels, we expect the lists to be sparse, so we can just search from head.
                    assert!(i > 1);
                    let head = self.get_head_mut();
                    let (_pre, _next) = self.find_splice_for_level(key, head, i as isize);
                    prev[i] = _pre;
                    if _next.is_some() {
                        next[i] = _next.unwrap() as *const Node;
                    }

                    // Someone adds the exact same key before we are able to do so. This can only happen on
                    // the base level. But we know we are not on the base level.
                    assert!(!ptr::eq(prev[i], next[i]));
                }

                let next_offset = self.arena_ref().get_node_offset(next[i]);
                x.tower[i].store(next_offset as u32, Ordering::SeqCst);
                if prev[i].as_ref().unwrap().cas_next_offset(
                    i,
                    next_offset as u32,
                    self.arena_ref().get_node_offset(x) as u32,
                ) {
                    // Managed to insert x between prev[i] and next[i]. Go to the next level.
                    break;
                }

                // CAS failed. We need to recompute prev and next.
                // It is unlikely to be helpful to try to use a different level as we redo the search,
                // because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
                let (_pre, _next) =
                    self.find_splice_for_level(key, prev[i].as_ref().unwrap(), i as isize);
                prev[i] = _pre;
                // FIXME: maybe nil pointer
                if _next.is_some() {
                    next[i] = _next.unwrap();
                } else {
                    next[i] = std::ptr::null();
                }
                if ptr::eq(prev[i], next[i]) {
                    assert_eq!(i, 0, "Equality can happen only on base level: {}", i);
                    prev[i]
                        .as_ref()
                        .unwrap()
                        .set_value(self.arena_mut_ref(), &v);
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
        let mut n = self.get_head() as *const Node;
        let mut level = self.get_height() - 1;
        loop {
            let next = self.get_next(&*n, level);
            if next.is_some() {
                n = next.unwrap();
                continue;
            }
            if level == 0 {
                if ptr::eq(n, self.get_head()) {
                    return None;
                }
                return Some(&*n);
            }
            level -= 1;
        }
    }

    // gets the value associated with the key.
    // FIXME: maybe return Option<&ValueStruct>
    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueStruct> {
        info!("find a key: {:?}", key);
        let (node, found) = self.find_near(key, false, true);
        if !found {
            return None;
        }
        let (value_offset, value_size) = node.unwrap().get_value_offset();
        Some(self.arena_ref().get_val(value_offset, value_size))
    }

    /// Returns a SkipList cursor. You have to close() the cursor.
    pub fn new_cursor(&self) -> Cursor<'_> {
        self._ref.fetch_add(1, Ordering::Relaxed);
        Cursor::new(self)
    }

    /// returns the size of the SkipList in terms of how much memory is used within its internal arena.
    pub fn mem_size(&self) -> u32 {
        self.arena_ref().size()
    }

    pub fn key_values(&self) -> Vec<(&[u8], ValueStruct)> {
        let mut cur = self.get_head().get_next_offset(0);
        let mut v = vec![];
        while cur > 0 {
            let node = self.arena_ref().get_node(cur as usize).unwrap();
            let key = node.key(self.arena_ref());
            let (value_offset, value_sz) = node.get_value_offset();
            let value = self.arena_ref().get_val(value_offset, value_sz);
            v.push((key, value));
            cur = node.get_next_offset(0);
        }
        v
    }

    fn random_height() -> usize {
        let mut h = 1;
        while h < MAX_HEIGHT && random::<u32>() <= HEIGHT_INCREASE {
            h += 1;
        }
        h
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        let _ref = self._ref.load(Ordering::Relaxed);
        info!("Drop SkipList, reference: {}", _ref);
    }
}

impl Display for SkipList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use tabled::{Table, Tabled};

        #[derive(Tabled)]
        struct KV {
            k: String,
            v: String,
        }
        let mut kv = vec![];
        for _kv in self.key_values() {
            kv.push(KV {
                k: String::from_utf8(_kv.0.to_vec()).unwrap(),
                v: String::from_utf8(_kv.1.value).unwrap(),
            });
        }
        let table = Table::new(kv).to_string();
        writeln!(f, "SkipList=>").unwrap();
        writeln!(f, "{}", table)
    }
}

// A unidirectional memetable iterator. It is a thin wrapper around
// `Iterator`. We like to keep `Iterator` as before, because it is more powerful and
// we might support bidirectional iterations in the  future.
pub struct UniIterator {
    iter: SkipIterator,
    reversed: bool,
}

impl UniIterator {
    pub fn new(st: SkipList, reversed: bool) -> UniIterator {
        let itr = SkipIterator::new(st);
        UniIterator {
            iter: itr,
            reversed,
        }
    }
}

impl Xiterator for UniIterator {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if !self.reversed {
            self.iter.prev()
        } else {
            self.iter.next()
        }
    }

    fn rewind(&self) -> Option<Self::Output> {
        if !self.reversed {
            self.iter.seek_to_first()
        } else {
            self.iter.seek_to_first()
        }
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        if !self.reversed {
            self.iter.seek(key)
        } else {
            self.iter.seek_to_prev(key)
        }
    }

    fn peek(&self) -> Option<Self::Output> {
        self.iter.peek()
    }

    fn close(&self) {
        self.iter.close()
    }
}

// An iterator over SkipList object. for new objects, you just
// need to initialize Iterator.list.
// Try GAT lifetime
pub struct SkipIterator {
    st: SkipList,
    node: AtomicPtr<Node>,
}

impl SkipIterator {
    pub fn new(st: SkipList) -> SkipIterator {
        SkipIterator {
            st,
            node: AtomicPtr::new(ptr::null_mut()),
        }
    }

    pub fn get_item_by_node(&self, node: &Node) -> Option<IteratorItem> {
        if ptr::eq(node, self.st.get_head()) {
            return None;
        }
        let key = node.key(&self.st.arena);
        let (value_offset, val_size) = node.get_value_offset();
        let value = self.st.arena_ref().get_val(value_offset, val_size);
        Some(IteratorItem {
            key: key.to_vec(),
            value,
        })
    }

    pub fn peek(&self) -> Option<IteratorItem> {
        unsafe {
            self.node
                .load(Ordering::Relaxed)
                .as_ref()
                .map(|node| self.get_item_by_node(node))
                .unwrap_or_else(|| None)
        }
    }

    // returns true iff the iterator is positioned at a valid node.
    pub fn valid(&self) -> bool {
        self.peek().is_some()
    }

    pub fn close(&self) {
        self.st.decr_ref()
    }

    // Advance to the next position
    pub fn next(&self) -> Option<IteratorItem> {
        let node = self.node.load(Ordering::Relaxed);
        if node.is_null() {
            return None;
        }
        let next = self.st.get_next(unsafe { node.as_ref().unwrap() }, 0);
        let next = next.unwrap() as *const Node as *mut Node;
        self.node.store(next, Ordering::Relaxed);
        self.get_item_by_node(unsafe { next.as_ref().unwrap() })
    }

    // Advances to the previous position.
    pub fn prev(&self) -> Option<IteratorItem> {
        assert!(self.peek().is_some());
        let (node, _) = self.st.find_near(self.peek().unwrap().key(), true, false);
        if node.is_none() {
            self.set_node(self.st.get_head());
            return None;
        }
        self.set_node(node.unwrap());
        self.get_item_by_node(node.unwrap())
    }

    // Advances to the first entry with a key >= target.
    pub fn seek(&self, key: &[u8]) -> Option<IteratorItem> {
        let (node, _) = self.st.find_near(key, false, true); // find >=.
        if node.is_none() {
            self.set_node(self.st.get_head());
            return None;
        }
        self.node
            .store(node.unwrap() as *const Node as *mut Node, Ordering::Relaxed);
        self.get_item_by_node(node.unwrap())
    }

    // finds an entry with key <= target.
    pub fn seek_to_prev(&self, key: &[u8]) -> Option<IteratorItem> {
        let (node, _) = self.st.find_near(key, true, true); // find <=1
        if node.is_none() {
            self.set_node(self.st.get_head());
            return None;
        }
        self.node
            .store(node.unwrap() as *const Node as *mut Node, Ordering::Relaxed);
        self.get_item_by_node(node.unwrap())
    }

    // Seeks position at the first entry in list.
    // Final state of iterator is valid() iff list is not empty.
    pub fn seek_to_first(&self) -> Option<IteratorItem> {
        let node = self.st.get_next(self.st.get_head(), 0);
        if node.is_none() {
            self.set_node(self.st.get_head());
            return None;
        }

        self.node
            .store(node.unwrap() as *const Node as *mut Node, Ordering::Relaxed);
        self.get_item_by_node(node.unwrap())
    }

    pub fn seek_to_last(&self) -> Option<IteratorItem> {
        let node = unsafe { self.st.find_last() };
        if node.is_none() {
            self.set_node(self.st.get_head());
            return None;
        }
        self.node
            .store(node.unwrap() as *const Node as *mut Node, Ordering::Relaxed);
        self.get_item_by_node(node.unwrap())
    }

    fn set_node(&self, node: &Node) {
        let node = node as *const Node as *mut Node;
        self.node.store(node, Ordering::Relaxed);
    }
}

mod tests {
    use crate::skl::node::Node;
    use crate::skl::skip::SkipList;
    use crate::skl::{Arena, Cursor, MAX_HEIGHT};
    use crate::y::ValueStruct;
    use rand::distributions::{Alphanumeric, DistString};
    use std::fmt::format;
    use std::ptr;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;
    use std::thread::spawn;

    const ARENA_SIZE: usize = 1 << 20;

    #[test]
    fn t_skip_list_size() {
        let mut st = SkipList::new(ARENA_SIZE);
        assert_eq!(st.height.load(Ordering::Relaxed), 1);
        assert_eq!(st._ref.load(Ordering::Relaxed), 1);
        let head = st.get_head();
        assert_eq!(head.height as usize, MAX_HEIGHT);
        assert_eq!(head.key_offset as usize, 1);
    }

    #[test]
    fn t_empty_list() {
        let mut st = SkipList::new(ARENA_SIZE);
        let key = b"aaa";
        let got = st.get(key);
        assert!(got.is_none());

        for (_, less) in vec![(true, false)] {
            for (_, allow_equal) in vec![(true, false)] {
                let found = st.find_near(key, less, allow_equal);
                assert!(found.0.is_none());
                assert_eq!(found.1, false);
            }
        }

        {
            let it = st.new_cursor();
            assert!(!it.valid());
            assert!(it.seek_for_first().is_none());
            assert!(it.seek_for_last().is_none());
            assert!(it.seek(key).is_none());
            it.close();
        }
        // Check the reference counting.
        assert!(st.valid());
    }

    // Tests single-threaded inserts and updates and gets.
    #[test]
    fn t_base() {
        let node = Node::default();
        let n1 = &node;
        let n2 = Some(&node);
        assert!(ptr::eq(n1, n2.unwrap()));
        let mut st = SkipList::new(ARENA_SIZE);
        let val1 = b"42";
        let val2 = b"52";
        let val3 = b"62";
        let val4 = b"72";
        // try inserting values.
        st.put(b"key1", ValueStruct::new(val1.to_vec(), 55, 0, 60000));
        st.put(b"key2", ValueStruct::new(val2.to_vec(), 56, 0, 60001));
        st.put(b"key3", ValueStruct::new(val3.to_vec(), 57, 0, 60002));

        println!("{}", st);

        let v = st.get(b"key");
        assert!(v.is_none());

        let v = st.get(b"key1");
        assert!(v.is_some());
        assert_eq!(v.unwrap(), ValueStruct::new(val1.to_vec(), 55, 0, 60000));

        let v = st.get(b"key2");
        assert!(v.is_some());
        assert_eq!(v.unwrap(), ValueStruct::new(val2.to_vec(), 56, 0, 60001));

        let v = st.get(b"key3");
        assert!(v.is_some());
        assert_eq!(v.unwrap(), ValueStruct::new(val3.to_vec(), 57, 0, 60002));

        st.put(b"key2", ValueStruct::new(val4.to_vec(), 12, 0, 50000));
        let v = st.get(b"key2").unwrap();
        assert_eq!(12, v.meta);
        assert_eq!(50000, v.cas_counter);
    }

    #[test]
    fn t_concurrent_basic() {
        use rand::{thread_rng, Rng};

        let st = Arc::new(SkipList::new(ARENA_SIZE));
        let mut kv = vec![];
        for i in 0..10000 {
            kv.push((
                Alphanumeric.sample_string(&mut rand::thread_rng(), 10),
                Alphanumeric.sample_string(&mut rand::thread_rng(), 20),
            ));
        }

        let mut waits = vec![];
        for (key, value) in kv.clone() {
            let st_ptr = st.clone();
            waits.push(spawn(move || {
                // let st = unsafe {st.as_ref()};
                st_ptr.put(
                    key.as_bytes(),
                    ValueStruct::new(value.as_bytes().to_vec(), 0, 0, 0),
                )
            }));
        }
        for join in waits {
            join.join().unwrap();
        }

        for (key, value) in kv {
            let got = st.get(key.as_bytes()).unwrap();
            assert_eq!(got.value, value.as_bytes().to_vec());
        }
    }

    fn t_concurrent_basic2() {
        use rand::{thread_rng, Rng};

        let st = SkipList::new(ARENA_SIZE);
        let mut kv = vec![];
        for i in 0..10000 {
            kv.push((
                Alphanumeric.sample_string(&mut rand::thread_rng(), 10),
                Alphanumeric.sample_string(&mut rand::thread_rng(), 20),
            ));
        }

        let mut waits = vec![];
        for (key, value) in kv.clone() {
            let st_ptr = st.clone();
            waits.push(spawn(move || {
                // let st = unsafe {st.as_ref()};
                st_ptr.put(
                    key.as_bytes(),
                    ValueStruct::new(value.as_bytes().to_vec(), 0, 0, 0),
                )
            }));
        }
        for join in waits {
            join.join().unwrap();
        }

        for (key, value) in kv {
            let got = st.get(key.as_bytes()).unwrap();
            assert_eq!(got.value, value.as_bytes().to_vec());
        }
    }

    #[test]
    fn t_one_key() {
        let key = "thekey";
        let st = Arc::new(SkipList::new(ARENA_SIZE));
        let mut waits = (0..100)
            .map(|i| {
                let st = st.clone();
                let key = key.clone();
                spawn(move || {
                    st.put(
                        key.as_bytes(),
                        ValueStruct::new(format!("{}", i).as_bytes().to_vec(), 0, 0, i as u64),
                    );
                })
            })
            .collect::<Vec<_>>();
        // We expect that at least some write made it such that some read returns a value.
        let save_value = Arc::new(AtomicI32::new(0));
        for i in 0..100 {
            let st = st.clone();
            let key = key.clone();
            let save_value = save_value.clone();
            let join = spawn(move || {
                if let Some(value) = st.get(key.as_bytes()) {
                    save_value.fetch_add(1, Ordering::Relaxed);
                    let v = String::from_utf8_lossy(&value.value)
                        .parse::<i32>()
                        .unwrap();
                    assert!(0 <= v && v < 100);
                    let cas_counter = value.cas_counter;
                    assert_eq!(v as u64, cas_counter);
                }
            });
            waits.push(join);
        }

        for join in waits {
            join.join().unwrap();
        }

        assert!(save_value.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn t_find_near() {
        let st = SkipList::new(ARENA_SIZE);
        for i in 0..1000 {
            let key = format!("{:05}", i * 10 + 5);
            st.put(
                key.as_bytes(),
                ValueStruct::new(i.to_string().as_bytes().to_vec(), 0, 0, i),
            );
        }

        let v = st.find_near(b"00001", false, false);
        assert!(v.0.is_some());
        assert_eq!(b"00005", v.0.unwrap().key(&st.arena));
        assert!(!v.1);
        let v = st.find_near(b"00001", false, true);
        assert!(v.0.is_some());
        assert_eq!(b"00005", v.0.unwrap().key(&st.arena));
        assert!(!v.1);
        let v = st.find_near(b"00001", true, false);
        assert!(v.0.is_none());
        assert!(!v.1);
        let v = st.find_near(b"00001", true, true);
        assert!(v.0.is_none());
        assert!(!v.1);

        let (n, eq) = st.find_near(b"00005", false, false);
        assert!(n.is_some());
        assert_eq!(b"00015", n.unwrap().key(&st.arena));
        assert!(!eq);
        let (n, eq) = st.find_near(b"00005", false, true);
        assert!(n.is_some());
        assert_eq!(b"00005", n.unwrap().key(&st.arena));
        assert!(eq);
        let (n, eq) = st.find_near(b"00005", true, false);
        assert!(n.is_none());
        assert!(!eq);
        let (n, eq) = st.find_near(b"00005", true, true);
        assert!(n.is_some());
        assert_eq!(b"00005", n.unwrap().key(&st.arena));
        assert!(eq);

        let (n, eq) = st.find_near(b"05555", false, false);
        assert!(n.is_some());
        assert_eq!(b"05565", n.unwrap().key(&st.arena));
        assert!(!eq);
        let (n, eq) = st.find_near(b"05555", false, true);
        assert!(n.is_some());
        assert_eq!(b"05555", n.unwrap().key(&st.arena));
        assert!(eq);
        let (n, eq) = st.find_near(b"05555", true, false);
        assert!(n.is_some());
        assert!(!eq);
        assert_eq!(b"05545", n.unwrap().key(&st.arena));
        let (n, eq) = st.find_near(b"05555", true, true);
        assert!(n.is_some());
        assert_eq!(b"05555", n.unwrap().key(&st.arena));
        assert!(eq);

        let (n, eq) = st.find_near(b"05558", false, false);
        assert!(n.is_some());
        assert_eq!(b"05565", n.unwrap().key(&st.arena));
        assert!(!eq);
        let (n, eq) = st.find_near(b"05558", false, true);
        assert!(n.is_some());
        assert_eq!(b"05565", n.unwrap().key(&st.arena));
        assert!(!eq);
        let (n, eq) = st.find_near(b"05558", true, false);
        assert!(n.is_some());
        assert!(!eq);
        assert_eq!(b"05555", n.unwrap().key(&st.arena));
        let (n, eq) = st.find_near(b"05558", true, true);
        assert!(n.is_some());
        assert_eq!(b"05555", n.unwrap().key(&st.arena));
        assert!(!eq);

        let (n, eq) = st.find_near(b"09995", false, false);
        assert!(n.is_none());
        assert!(!eq);
        let (n, eq) = st.find_near(b"09995", false, true);
        assert!(n.is_some());
        assert_eq!(b"09995", n.unwrap().key(&st.arena));
        assert!(eq);
        let (n, eq) = st.find_near(b"09995", true, false);
        assert!(n.is_some());
        assert!(!eq);
        assert_eq!(b"09985", n.unwrap().key(&st.arena));
        let (n, eq) = st.find_near(b"09995", true, true);
        assert!(n.is_some());
        assert_eq!(b"09995", n.unwrap().key(&st.arena));
        assert!(eq);

        let (n, eq) = st.find_near(b"59995", false, false);
        assert!(n.is_none());
        assert!(!eq);
        let (n, eq) = st.find_near(b"59995", false, true);
        assert!(n.is_none());
        assert!(!eq);
        let (n, eq) = st.find_near(b"59995", true, false);
        assert!(n.is_some());
        assert!(!eq);
        assert_eq!(b"09995", n.unwrap().key(&st.arena));
        let (n, eq) = st.find_near(b"59995", true, true);
        assert!(n.is_some());
        assert_eq!(b"09995", n.unwrap().key(&st.arena));
        assert!(!eq);
    }

    // Tests a basic iteration over all nodes from the beginning.
    #[test]
    fn t_iterator_next() {
        const n: usize = 100;
        let st = SkipList::new(ARENA_SIZE);

        {
            let cur = st.new_cursor();
            assert!(!cur.valid());
            cur.seek_for_first();
            assert!(!cur.valid());

            for i in 0..n {
                let key = format!("{:05}", i);
                st.put(
                    key.as_bytes(),
                    ValueStruct::new(key.as_bytes().to_vec(), 0, 0, i as u64),
                );
            }

            cur.seek_for_first();
            for i in 0..n {
                assert!(cur.valid());
                let v = cur.value();
                assert_eq!(v.value, format!("{:05}", i).as_bytes().to_vec());
                cur.next();
            }

            assert!(!cur.valid());
            cur.close();
        }
    }

    #[test]
    fn t_iterator_prev() {
        const n: usize = 100;
        let st = SkipList::new(ARENA_SIZE);
        let cur = st.new_cursor();
        assert!(!cur.valid());
        cur.seek_for_first();
        assert!(!cur.valid());

        for i in 0..100 {
            let key = format!("{:05}", i);
            st.put(
                key.as_bytes(),
                ValueStruct::new(key.as_bytes().to_vec(), 0, 0, i as u64),
            );
        }
        cur.seek_for_last();

        for i in (0..100).rev() {
            assert!(cur.valid());
            let v = cur.value();
            assert_eq!(v.value, format!("{:05}", i).as_bytes().to_vec());
            cur.prev();
        }

        assert!(!cur.valid());
        cur.close();
    }

    #[test]
    fn t_iterator_seek() {
        const n: usize = 100;
        let st = SkipList::new(ARENA_SIZE);
        let cur = st.new_cursor();
        assert!(!cur.valid());

        cur.seek_for_first();
        assert!(!cur.valid());
        for i in 0..n {
            let key = format!("{:05}", i * 10 + 1000);
            st.put(
                key.as_bytes(),
                ValueStruct::new(key.as_bytes().to_vec(), 0, 0, 555),
            );
        }
        cur.seek(b"");
        assert!(cur.valid());
        assert_eq!(b"01000", cur.value().value.as_slice());

        cur.seek(b"01000");
        assert!(cur.valid());
        assert_eq!(b"01000", cur.value().value.as_slice());

        cur.seek(b"01005");
        assert!(cur.valid());
        assert_eq!(b"01010", cur.value().value.as_slice());

        cur.seek(b"01010");
        assert!(cur.valid());
        assert_eq!(b"01010", cur.value().value.as_slice());

        cur.seek(b"99999");
        assert!(!cur.valid());

        // Try seek_for_prev
        cur.seek_for_prev(b"");
        assert!(!cur.valid());
        cur.seek_for_prev(b"01000");
        assert!(cur.valid());
        assert_eq!(b"01000", cur.value().value.as_slice());

        cur.seek_for_prev(b"01005");
        assert!(cur.valid());
        assert_eq!(b"01000", cur.value().value.as_slice());

        cur.seek_for_prev(b"01010");
        assert!(cur.valid());
        assert_eq!(b"01010", cur.value().value.as_slice());

        cur.seek_for_prev(b"99999");
        assert!(cur.valid());
        assert_eq!(b"01990", cur.value().value.as_slice());
        cur.close();
    }
}

mod tests2 {
    use crate::{SkipList, ValueStruct};

    const ARENA_SIZE: usize = 1 << 20;

    #[test]
    fn atomic_swap_skip_list() {
        let mut st = SkipList::new(ARENA_SIZE);
        st.put(b"hello", ValueStruct::new(vec![], 0, 0, 0));
        let got = st.get(b"hello");
        assert!(got.is_some());
    }
}
