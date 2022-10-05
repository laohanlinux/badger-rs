use crate::skl::{node::Node, skip::SkipList, Chunk};
use crate::y::ValueStruct;
use crate::BadgerErr;
use serde_json::Value;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::mem::take;
use std::ops::Deref;

/// An iterator over `SkipList` object. For new objects, you just
/// need to initialize Iterator.List.
pub struct Cursor<'a> {
    pub(crate) list: &'a SkipList,
    item: RefCell<Option<&'a Node>>,
}

impl<'a> Cursor<'a> {
    pub fn new(list: &'a SkipList) -> Cursor<'a> {
        Cursor {
            list,
            item: RefCell::new(None),
        }
    }

    /// Returns true if the iterator is positioned at a valid node.
    pub fn valid(&self) -> bool {
        self.item.borrow().is_some()
    }

    /// Returns the key at the current position.
    pub fn key(&self) -> &[u8] {
        let node = self.item.borrow().unwrap();
        self.list
            .arena_ref()
            .get_key(node.key_offset, node.key_size)
    }

    /// Return value.
    pub fn value(&self) -> ValueStruct {
        let node = self.item.borrow().unwrap();
        let (value_offset, val_size) = node.get_value_offset();
        self.list.arena_ref().get_val(value_offset, val_size)
    }

    /// Advances to the next position.
    pub fn next(&'a self) -> Option<&Node> {
        assert!(self.valid());
        let next = self.list.get_next(self.item.borrow().unwrap(), 0);
        *self.item.borrow_mut() = next;
        next
    }

    /// Advances to the previous position.
    pub fn prev(&'a self) -> Option<&Node> {
        assert!(self.valid());
        let (node, _) = self.list.find_near(self.key(), true, false);
        *self.item.borrow_mut() = node;
        node
    }

    /// Advance to the first entry with a key >= target.
    pub fn seek(&'a self, target: &[u8]) -> Option<&Node> {
        let (node, _) = self.list.find_near(target, false, true); // find >=.
        *self.item.borrow_mut() = node;
        node
    }

    /// Finds an entry with key <= target.
    pub fn seek_for_prev(&'a self, target: &[u8]) -> Option<&Node> {
        let (node, _) = self.list.find_near(target, true, true); // find <=.
        *self.item.borrow_mut() = node;
        node
    }

    /// Seeks position at the first entry in list.
    /// Final state of iterator is Valid() iff list is not empty.
    pub fn seek_for_first(&'a self) -> Option<&'a Node> {
        let node = self.list.get_next(self.list.get_head(), 0);
        *self.item.borrow_mut() = node;
        node
    }

    /// Seeks position at the last entry in list.
    /// Final state of iterator is Valid() iff list is not empty.
    pub fn seek_for_last(&'a self) -> Option<&Node> {
        let node = unsafe { self.list.find_last() };
        *self.item.borrow_mut() = node;
        node
    }

    // Must be call for every `Cursor`
    pub fn close(&self) {
        self.list.decr_ref();
    }
}

// impl<'a> Drop for Cursor<'a> {
//     fn drop(&mut self) {
//         println!("drop cursor");
//         self.list.deref();
//     }
// }
//
// pub struct CursorReverse<'a> {
//     iter: &'a Cursor<'a>,
//     reversed: RefCell<bool>,
// }
//
// impl<'a> CursorReverse<'a> {
//     pub fn next(&self) -> Option<&Node> {
//         if !*self.reversed.borrow() {
//             self.iter.next()
//         } else {
//             self.iter.prev()
//         }
//     }
//
//     pub fn rewind(&self) -> Option<&Node> {
//         if !*self.reversed.borrow() {
//             self.iter.seek_for_first()
//         } else {
//             self.iter.seek_for_last()
//         }
//     }
//
//     pub fn seek(&self, key: &[u8]) -> Option<&Node> {
//         if !*self.reversed.borrow() {
//             self.iter.seek(key)
//         } else {
//             self.iter.seek_for_prev(key)
//         }
//     }
//
//     pub fn key(&self) -> &[u8] {
//         self.iter.key()
//     }
//
//     pub fn value(&self) -> ValueStruct {
//         self.iter.value()
//     }
//
//     pub fn valid(&self) -> bool {
//         self.valid()
//     }
//
//     pub fn close(&self) {
//         todo!()
//     }
// }
//
// //
// impl<'a> UniIterator<'a> {
//     pub fn next(&self) {
//         todo!()
//     }
//
//     pub fn rewind(&self) {
//         todo!()
//     }
//
//     pub fn seek(&self, key: &[u8]) {
//         todo!()
//     }
//
//     pub fn key(&self) {
//         todo!()
//     }
//
//     pub fn value(&self) -> ValueStruct {
//         todo!()
//     }
//
//     pub fn valid(&self) -> bool {
//         todo!()
//     }
//
//     pub fn close(&self) { todo!() }
// }

#[test]
fn t_cursor() {}
