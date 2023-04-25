use crate::table::iterator::IteratorItem;
use crate::y::iterator::{KeyValue, Xiterator};
use crate::y::ValueStruct;
use log::info;
use std::cell::{Ref, RefCell};
use std::fmt;
use std::fmt::Formatter;

pub struct MergeIterElement {
    index: usize,
}

/// Cursor of the merge's iterator.
pub struct MergeIterCursor {
    pub is_dummy: bool,
    pub cur_item: Option<IteratorItem>,
}

/// Merge iterator,
pub struct MergeIterOverIterator {
    pub reverse: bool,
    pub all: Vec<Box<dyn Xiterator<Output=IteratorItem>>>,
    pub elements: RefCell<Vec<usize>>,
    pub cursor: RefCell<MergeIterCursor>,
}

impl Xiterator for MergeIterOverIterator {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.cursor.borrow().is_dummy {
            self.store_key(None);
            return self.rewind();
        }
        if self.elements.borrow().is_empty() {
            self.store_key(None);
            return None;
        }
        println!("elements size: {}", self.elements.borrow().len());
        self.move_cursor()
    }

    fn rewind(&self) -> Option<Self::Output> {
        for itr in self.all.iter() {
            itr.rewind();
        }
        self.reset();
        self.move_cursor()
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        for iter in self.all.iter() {
            iter.seek(key);
        }
        self.reset();
        self.peek()
    }

    fn peek(&self) -> Option<Self::Output> {
        self.cursor.borrow().cur_item.clone()
    }

    fn close(&self) {
        self.all.iter().for_each(|itr| itr.close());
    }
}

impl MergeIterOverIterator {
    fn reset(&self) {
        self.elements.borrow_mut().clear();
        for (index, itr) in self.all.iter().enumerate() {
            if itr.peek().is_none() {
                continue;
            }
            self.elements.borrow_mut().push(index);
        }

        self.elements.borrow_mut().sort_unstable_by(|a, b| {
            let a_itr = self.all.get(*a).unwrap();
            let b_itr = self.all.get(*b).unwrap();
            if a_itr.peek().unwrap().key() == b_itr.peek().unwrap().key() {
                return a.cmp(b);
            }
            a_itr.peek().unwrap().key().cmp(b_itr.peek().unwrap().key())
        });
        println!("sets: {:?}", self.elements.borrow());
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

    fn get_iter(&self, index: usize) -> &Box<dyn Xiterator<Output=IteratorItem>> {
        self.all.get(index).unwrap()
    }

    fn store_key(&self, item: Option<IteratorItem>) {
        self.cursor.borrow_mut().is_dummy = item.is_none();
        self.cursor.borrow_mut().cur_item = item;
    }
}

#[derive(Default)]
pub struct MergeIterOverBuilder {
    all: Vec<Box<dyn Xiterator<Output=IteratorItem>>>,
    reverse: bool,
}

impl MergeIterOverBuilder {
    pub fn add(mut self, x: Box<dyn Xiterator<Output=IteratorItem>>) -> MergeIterOverBuilder {
        self.all.push(x);
        self
    }

    pub fn add_batch(
        mut self,
        iters: Vec<Box<dyn Xiterator<Output=IteratorItem>>>,
    ) -> MergeIterOverBuilder {
        self.all.extend(iters);
        self
    }

    pub fn build(mut self) -> MergeIterOverIterator {
        MergeIterOverIterator {
            all: self.all,
            reverse: self.reverse,
            cursor: RefCell::new(MergeIterCursor {
                is_dummy: true,
                cur_item: None,
            }),
            elements: RefCell::new(vec![]),
        }
    }
}

#[test]
fn merge_iter_element() {
    #[derive(Debug)]
    struct TestIterator {
        key: Vec<u8>,
    }

    impl fmt::Display for TestIterator {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "key: {}", String::from_utf8_lossy(&self.key))
        }
    }

    impl Xiterator for TestIterator {
        type Output = IteratorItem;

        fn next(&self) -> Option<Self::Output> {
            self.peek()
        }

        fn rewind(&self) -> Option<Self::Output> {
            self.peek()
        }

        fn seek(&self, key: &[u8]) -> Option<Self::Output> {
            self.peek()
        }

        fn peek(&self) -> Option<Self::Output> {
            Some(IteratorItem::new(self.key.clone(), ValueStruct::default()))
        }
    }

    impl KeyValue<ValueStruct> for TestIterator {
        fn key(&self) -> &[u8] {
            &self.key
        }

        fn value(&self) -> ValueStruct {
            todo!()
        }
    }

    let t1 = Box::new(TestIterator {
        key: b"abd".to_vec(),
    });
    let t2 = Box::new(TestIterator {
        key: b"abc".to_vec(),
    });
    let t3 = Box::new(TestIterator {
        key: b"abc".to_vec(),
    });
    let t4 = Box::new(TestIterator {
        key: b"abc".to_vec(),
    });

    let builder = MergeIterOverBuilder::default().add_batch(vec![t3, t1, t4, t2]);
    let miter = builder.build();
    miter.next();
    assert_eq!(miter.peek().unwrap().key(), b"abc");
}
