use crate::skl::{HEIGHT_INCREASE, MAX_HEIGHT};
use crate::{Node, ValueStruct};
use std::sync::atomic::{AtomicI32, AtomicPtr, Ordering};
use std::sync::Arc;
use rand::random;

/// SkipList
pub struct SkipList {
    height: Arc<AtomicI32>,
    head: AtomicPtr<Node>,
    _ref: Arc<AtomicI32>,
}

impl Clone for SkipList {
    fn clone(&self) -> Self {
        let node = self.head.load(Ordering::Relaxed);
        SkipList {
            height: self.height.clone(),
            head: AtomicPtr::new(node),
            _ref: self._ref.clone(),
        }
    }
}

impl SkipList {
    pub fn new(arena_size: usize) -> Self {
        let v = ValueStruct::default();
        let node = Node::new2(
            "".as_bytes().to_vec(),
            "".as_bytes().to_vec(),
            MAX_HEIGHT as isize,
        );
        Self {
            height: Arc::new(AtomicI32::new(1)),
            head: AtomicPtr::new(Box::into_raw(Box::new(node))),
            _ref: Arc::new(AtomicI32::new(1)),
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
        todo!()
    }

    pub(crate) fn get_head(&self) -> &Node {
        unsafe { &*(self.head.load(Ordering::Relaxed) as *const Node) }
    }

    fn get_head_mut(&self) -> &mut Node {
        let node = unsafe { self.head.load(Ordering::Relaxed) as *mut Node };
        unsafe { &mut *node }
    }

    // pub(crate) fn get_next(&self, nd: &Node, height: isize) -> Option<&Node> {
    //     self.arena_ref()
    //         .get_node(nd.get_next_offset(height as usize) as usize)
    // }

    fn get_height(&self) -> isize {
        self.height.load(Ordering::Relaxed) as isize
    }


    // pub fn key_values(&self) -> Vec<(&[u8], ValueStruct)> {
    //     let mut cur = self.get_head().get_next_offset(0);
    //     let mut v = vec![];
    //     while cur > 0 {
    //         let node = self.arena_ref().get_node(cur as usize).unwrap();
    //         let key = node.key(self.arena_ref());
    //         let (value_offset, value_sz) = node.get_value_offset();
    //         let value = self.arena_ref().get_val(value_offset, value_sz);
    //         v.push((key, value));
    //         cur = node.get_next_offset(0);
    //     }
    //     v
    // }

    fn random_height() -> usize {
        let mut h = 1;
        while h < MAX_HEIGHT && random::<u32>() <= HEIGHT_INCREASE {
            h += 1;
        }
        h
    }
}
