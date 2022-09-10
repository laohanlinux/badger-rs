use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Deref;
use serde_json::Value;
use crate::skl::{Chunk, Node, SkipList};
use crate::y::ValueStruct;

/// An iterator over `SkipList` object. For new objects, you just
/// need to initialize Iterator.List.
pub struct Cursor<'a, ST: Deref<Target=SkipList>> {
    pub(crate) list: ST,
    item: RefCell<CursorItem<'a>>,
}

pub struct CursorItem<'a> {
    pub value: Option<&'a Node>,
}

impl<'a> CursorItem<'a> {
    fn close(&self) {
        todo!()
    }
    fn is_null(&self) -> bool { self.value.is_none() }
}

impl<'a, ST> Cursor<'a, ST> where ST: Deref<Target=SkipList> {
    /// Returns true if the iterator is positioned at a valid node.
    pub fn is_valid(&self) -> bool { self.item.borrow().is_null() }

    /// Returns the key at the current position.
    pub fn key(&self) -> impl Chunk {
        self.item.borrow().value.unwrap().key(&self.list.arena)
    }

    /// Return value.
    pub fn value(&self) -> ValueStruct {
        let (value_offset, val_size) = self.item.borrow().value.unwrap().get_value_offset();
        self.list.arena.get_val(value_offset, val_size)
    }

    /// Advances to the next position.
    pub fn next(&self) {
        assert!(self.is_valid());
        let next = self.list.get_next(self.item.borrow().value.unwrap(), 0);
        // self.item.value = next;
        todo!()
    }

    pub fn prev(&self) {
        todo!()
    }

    pub fn seek(&self, target: &[u8]) {
        todo!()
    }

    pub fn seek_for_prev(&self, target: &[u8]) {
        todo!()
    }

    pub fn seek_for_first(&self, target: &[u8]) {
        todo!()
    }

    pub fn seek_for_last(&self, target: &[u8]) {
        todo!()
    }
}

pub struct UniIterator<'a> {
    iter: &'a CursorItem<'a>,
    reversed: bool,
}

impl<'a> UniIterator<'a> {
    pub fn next(&self) {
        todo!()
    }

    pub fn rewind(&self) {
        todo!()
    }

    pub fn seek(&self, key: &[u8]) {
        todo!()
    }

    pub fn key(&self) {
        todo!()
    }

    pub fn value(&self) -> ValueStruct {
        todo!()
    }

    pub fn valid(&self) -> bool {
        todo!()
    }

    pub fn close(&self) { todo!() }
}

#[test]
fn t_cursor() {

}