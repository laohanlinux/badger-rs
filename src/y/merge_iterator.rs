use crate::table::iterator::IteratorItem;
use crate::y::iterator::Xiterator;
use std::cell::RefCell;

pub struct MergeIterElement {
    index: usize,
}

pub struct MergeIterCursor {
    is_dummy: bool,
    cur_item: Option<IteratorItem>,
}

#[derive(Default)]
pub struct MergeIterOverIterator {
    pub reverse: bool,
    pub all: Vec<Box<dyn Xiterator<Output = IteratorItem>>>,
    pub elements: RefCell<Vec<usize>>,
    pub cursor: RefCell<MergeIterCursor>,
}

impl Xiterator for MergeIterOverIterator {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        todo!()
    }

    fn rewind(&self) -> Option<Self::Output> {
        todo!()
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        todo!()
    }

    fn peek(&self) -> Option<Self::Output> {
        self.cursor.borrow().cur_item.clone()
    }

    fn close(&self) {
        todo!()
    }
}

impl MergeIterOverIterator {
    fn reset(&self) {
        self.elements.borrow_mut().clear();

    }

    fn move_cursor(&self) -> Option<IteratorItem> {
        let mut item = None;
        // Move same item
        for itr in self.elements.borrow().iter() {
            let cur_iter = self.get_iter(*itr);
            let itr_item = cur_iter.peek();
            if itr_item.is_none() {
                break;
            }
            if item.is_none() {
                item = itr_item.clone();
                cur_iter.next();
                continue;
            }
            if itr_item.as_ref().unwrap().key() != item.as_ref().unwrap().key() {
                break;
            }
            cur_iter.next();
        }
        self.store_key(item.clone());
        self.reset();
        item
    }

    fn get_iter(&self, index: usize) -> &Box<dyn Xiterator<Output = IteratorItem>> {
        self.all.get(index).unwrap()
    }

    fn store_key(&self, item: Option<IteratorItem>) {
        self.cursor.borrow_mut().is_dummy = item.is_none();
        self.cursor.borrow_mut().cur_item = item;
    }
}
