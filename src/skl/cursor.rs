use crate::skl::{Chunk, Node, SkipList};
use crate::y::ValueStruct;
use crate::BadgerErr;
use serde_json::Value;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Deref;

/// An iterator over `SkipList` object. For new objects, you just
/// need to initialize Iterator.List.
pub struct Cursor<'a, ST: Deref<Target = SkipList>> {
    pub(crate) list: ST,
    item: RefCell<Option<&'a Node>>,
}

impl<'a, ST> Cursor<'a, ST>
where
    ST: Deref<Target = SkipList>,
{
    /// Returns true if the iterator is positioned at a valid node.
    pub fn is_valid(&self) -> bool {
        self.item.borrow().is_some()
    }

    /// Returns the key at the current position.
    pub fn key(&self) -> impl Chunk {
        let node = self.item.borrow().unwrap();
        self.list.arena.get_key(node.key_offset, node.key_size)
    }

    /// Return value.
    pub fn value(&self) -> ValueStruct {
        let node = self.item.borrow().unwrap();
        let (value_offset, val_size) = node.get_value_offset();
        self.list.arena.get_val(value_offset, val_size)
    }

    /// Advances to the next position.
    pub fn next(&'a self) -> Option<&Node> {
        assert!(self.is_valid());
        let next = self.list.get_next(self.item.borrow().unwrap(), 0);
        *self.item.borrow_mut() = next;
        next
    }

    // Advances to the previous position.
    pub fn prev(&'a self) -> Option<&Node> {
        assert!(self.is_valid());
        let (node, _) = self.list.find_near(self.key().get_data(), true, false);
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
    pub fn seek_for_first(&'a self, target: &[u8]) -> Option<&Node> {
        let head = self.list.head.borrow();
        let node = self.list.get_next(&head, 0);
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
}

// pub struct UniIterator<'a> {
//     iter: &'a CursorItem<'a>,
//     reversed: bool,
// }
//
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
