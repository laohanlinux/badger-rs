use crate::table::iterator::IteratorItem;
use crate::types::TArcMx;
use crate::y::iterator::Xiterator;
use crate::y::{KeyValue, ValueStruct};
use crate::{SkipIterator, SkipList, UniIterator};
use log::info;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;

/// Cursor of the iterator of merge.
pub struct MergeIterCursor {
    pub is_dummy: bool,
    pub cur_item: Option<IteratorItem>,
}

/// Merge iterator,
pub struct MergeIterOverIterator {
    pub reverse: bool,
    pub all: Vec<Box<dyn Xiterator<Output = IteratorItem>>>,
    pub elements: RefCell<Vec<usize>>,
    pub cursor: RefCell<MergeIterCursor>,
}

impl Xiterator for MergeIterOverIterator {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.cursor.borrow().is_dummy {
            self.store_key(None);
            // self.cursor.borrow_mut().is_dummy = false;
            return self.rewind();
        }
        if self.elements.borrow().is_empty() {
            self.store_key(None);
            return None;
        }
        //println!("elements size: {}", self.elements.borrow().len());
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

    // TODO look like Skip some item
    fn peek(&self) -> Option<Self::Output> {
        println!("element size: {}", self.elements.borrow().len());
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
            // THIS is a test code
            if itr.peek().is_none() {
                let key = itr.seek(b"k00003_00000008");
                let key = key.as_ref().unwrap().key();
                if key == b"k00003_00000008" {
                    let cur = self.peek();
                    println!(
                        "find it, {}, cur key {} ",
                        index,
                        String::from_utf8_lossy(cur.as_ref().unwrap().key())
                    );
                    itr.rewind();
                    let mut find = false;
                    let mut last = vec![];
                    while let Some(item) = itr.next() {
                        if item.key() == b"k00003_00000008" {
                            find = true;
                        }
                        last = item.key.clone();
                        println!(">>>>>>>>>>>>>>>>>>>");
                    }
                }
                println!("delete skip, {}", index);
                continue;
            }
            self.elements.borrow_mut().push(index);
        }

        self.elements.borrow_mut().sort_by(|a, b| {
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
                println!("iter is empty");
                break;
            }
            if item.is_none() {
                println!(">> iter is empty");
                item = itr_item.clone();
                cur_iter.next();
                continue;
            }
            if itr_item.as_ref().unwrap().key() != item.as_ref().unwrap().key() {
                break;
            }
            cur_iter.next();
        }
        println!("{:?}", item);
        self.store_key(item.clone());
        self.reset();
        item
    }

    fn get_iter(&self, index: usize) -> &Box<dyn Xiterator<Output = IteratorItem>> {
        self.all.get(index).unwrap()
    }

    fn store_key(&self, item: Option<IteratorItem>) {
        // TODO notice is_dummy check, If the iter execute to last, is_dummy?
        self.cursor.borrow_mut().is_dummy = item.is_none();
        self.cursor.borrow_mut().cur_item = item;
    }
}

#[derive(Default)]
pub struct MergeIterOverBuilder {
    all: Vec<Box<dyn Xiterator<Output = IteratorItem>>>,
    reverse: bool,
}

impl MergeIterOverBuilder {
    pub fn add(mut self, x: Box<dyn Xiterator<Output = IteratorItem>>) -> MergeIterOverBuilder {
        self.all.push(x);
        self
    }

    pub fn add_batch(
        mut self,
        iters: Vec<Box<dyn Xiterator<Output = IteratorItem>>>,
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

#[tokio::test]
async fn merge_iter_skip() {
    let st1 = SkipList::new(1 << 20);
    let st2 = SkipList::new(1 << 20);
    let st3 = SkipList::new(1 << 20);
    let mut wg = awaitgroup::WaitGroup::new();
    let keys = TArcMx::new(tokio::sync::Mutex::new(vec![]));
    let n = 300;
    let m = 10;
    for i in 0..n {
        let wk = wg.worker();
        let keys = keys.clone();
        let st1 = st1.clone();
        let st2 = st2.clone();
        let st3 = st3.clone();
        tokio::spawn(async move {
            for j in 0..m {
                let key = format!("k{:05}_{:08}", i, j).into_bytes().to_vec();
                keys.lock().await.push(key.clone());
                if j % 3 == 0 {
                    st1.put(&key, ValueStruct::new(key.clone(), 0, 0, 0));
                } else if j % 3 == 1 {
                    st2.put(&key, ValueStruct::new(key.clone(), 0, 0, 0));
                } else {
                    st3.put(&key, ValueStruct::new(key.clone(), 0, 0, 0));
                }
            }
            wk.done();
        });
    }

    wg.wait().await;
    println!("{}", st1.node_count());
    println!("{}", st2.node_count());
    println!("{}", st3.node_count());
    assert_eq!(
        st1.node_count() + st2.node_count() + st3.node_count(),
        m * n
    );
    let builder = MergeIterOverBuilder::default()
        // .add(Box::new(UniIterator::new(st1, false)))
        // .add(Box::new(UniIterator::new(st2, false)))
        .add(Box::new(UniIterator::new(st3, false)));

    let miter = builder.build();
    let mut count = 0;
    let mut got = HashSet::new();
    while let Some(value) = miter.next() {
        count += 1;
        got.insert(value.key);
        if count == m * n {
            break;
        }
    }
    assert_eq!(count, m *n);
    assert_eq!(count, got.len() as u32);
}
