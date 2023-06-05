use crate::table::iterator::IteratorItem;
use crate::types::TArcMx;
use crate::y::iterator::Xiterator;
use crate::y::{KeyValue, ValueStruct};
use crate::{SkipIterator, SkipList, UniIterator};
use log::{debug, info};
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;

/// Cursor of the iterator of merge.
pub struct MergeIterCursor {
    pub is_dummy: bool,
    pub cur_item: Option<IteratorItem>,
}

pub struct MergeCursor {
    pub index: usize,
    pub cur_item: Option<IteratorItem>,
}

impl MergeCursor {
    fn replace(&mut self, index: usize, cur_item: Option<IteratorItem>) {
        self.index = index;
        self.cur_item = cur_item;
    }
}

pub struct MergeIterator {
    pub reverse: bool,
    pub itrs: Vec<Box<dyn Xiterator<Output=IteratorItem>>>,
    pub cursor: RefCell<MergeCursor>,
}

impl Xiterator for MergeIterator {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.itrs.is_empty() {
            return None;
        }
        if self.cursor.borrow().index == usize::MAX {
            for itr in &self.itrs {
                itr.rewind();
            }
            self.cursor.borrow_mut().index = 0;
        }
        let mut latest: Option<IteratorItem> = None;
        let mut index = usize::MAX;
        let need_cmp = |a: &[u8], b: &[u8]| -> std::cmp::Ordering {
            if !self.reverse {
                return a.cmp(b);
            }
            b.cmp(a)
        };

        for (itr_index, itr) in self.itrs.iter().enumerate() {
            if let Some(item) = itr.peek() {
                if let Some(have_latest) = &mut latest {
                    match need_cmp(have_latest.key(), item.key()) {
                        std::cmp::Ordering::Less => {
                            // debug!("less");
                        }
                        std::cmp::Ordering::Equal => {
                            // move it
                            itr.next();
                            // debug!("equal");
                        }
                        std::cmp::Ordering::Greater => {
                            index = itr_index;
                            *have_latest = item;
                            // debug!("greater");
                        }
                    }
                } else {
                    latest.replace(item);
                    index = itr_index;
                    // debug!("None");
                }
            }
        }

        // info!("Do one {}", index);
        if index != usize::MAX {
            assert!(latest.is_some());
            self.cursor.borrow_mut().replace(index, latest);
            self.itrs.get(index).as_ref().unwrap().next();
            // debug!("move it");
        } else {
            self.cursor.borrow_mut().index = 0;
            self.cursor.borrow_mut().cur_item.take();
        }
        self.peek()
    }

    /// Notice: Only rewind inner iterators list. Not reverse self.itrs vec sequence
    fn rewind(&self) -> Option<Self::Output> {
        if self.itrs.is_empty() {
            return None;
        }
        for itr in self.itrs.iter() {
            itr.rewind();
        }

        let mut min_or_max = None;
        if !self.reverse {
            min_or_max = self.itrs.iter().max_by_key(|itr| {
                if let Some(item) = self.peek() {
                    return item.key;
                } else {
                    vec![]
                }
            });
        } else {
            min_or_max = self.itrs.iter().min_by_key(|itr| {
                if let Some(item) = self.peek() {
                    return item.key;
                } else {
                    vec![]
                }
            });
        }
        assert!(min_or_max.is_some());
        let min_or_max = min_or_max.unwrap().peek();
        assert!(min_or_max.is_some());
        let min_or_max_item = min_or_max.unwrap();

        let mut update = false;
        let mut indexes = (0..self.itrs.len()).into_iter().collect::<Vec<_>>();

        for index in indexes {
            let itr = &self.itrs[index];
            if let Some(item) = itr.peek() && item.key() == min_or_max_item.key() {
                if !update {
                    self.cursor.borrow_mut().replace(index, Some(item));
                }
                update = true;
                itr.next();
            }
        }
        self.peek()
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        todo!()
    }

    // MayBe we avoid copy!
    fn peek(&self) -> Option<Self::Output> {
        self.cursor.borrow().cur_item.clone()
    }
}

impl std::iter::Iterator for MergeIterator {
    type Item = IteratorItem;

    fn next(&mut self) -> Option<Self::Item> {
        Xiterator::next(self)
    }
}

impl MergeIterator {
    fn find_smallest_or_biggest(&self) -> Option<(usize, Vec<usize>)> {
        if self.itrs.is_empty() {
            return None;
        }
        let mut latest: Option<IteratorItem> = None;
        let mut index = usize::MAX;
        let need_cmp = |a: &[u8], b: &[u8]| -> std::cmp::Ordering {
            if !self.reverse {
                return a.cmp(b);
            }
            b.cmp(a)
        };
        let mut eq_itr = Vec::with_capacity(self.itrs.len());
        for (itr_index, itr) in self.itrs.iter().enumerate() {
            if let Some(item) = itr.peek() {
                if let Some(have_latest) = &mut latest {
                    match need_cmp(have_latest.key(), item.key()) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => {
                            eq_itr.push(itr_index);
                        }
                        std::cmp::Ordering::Greater => {
                            index = itr_index;
                            *have_latest = item;
                        }
                    }
                } else {
                    latest.replace(item);
                    index = itr_index;
                }
            }
        }
        if index != usize::MAX {
            return Some((index, eq_itr));
        }
        None
    }
}

#[derive(Default)]
pub struct MergeIterOverBuilder {
    all: Vec<Box<dyn Xiterator<Output=IteratorItem>>>,
    reverse: bool,
}

impl MergeIterOverBuilder {
    pub fn reverse(mut self, reverse: bool) -> MergeIterOverBuilder {
        self.reverse = reverse;
        self
    }

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

    pub fn build(mut self) -> MergeIterator {
        MergeIterator {
            reverse: self.reverse,
            itrs: self.all,
            cursor: RefCell::new(MergeCursor {
                index: usize::MAX,
                cur_item: None,
            }),
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
    crate::test_util::tracing_log();
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
    assert_eq!(
        st1.node_count() + st2.node_count() + st3.node_count(),
        m * n
    );
    keys.lock().await.sort();
    // reverse = false
    {
        let builder = MergeIterOverBuilder::default()
            .add(Box::new(UniIterator::new(st1.clone(), false)))
            .add(Box::new(UniIterator::new(st2.clone(), false)))
            .add(Box::new(UniIterator::new(st3.clone(), false)));
        let miter = builder.build();
        assert!(miter.peek().is_none());
        let mut count = 0;
        while let Some(value) = miter.next() {
            // info!("{}", String::from_utf8_lossy(value.key()));
            let expect = keys.lock().await;
            let expect = expect.get(count).unwrap();
            assert_eq!(value.key(), expect);
            count += 1;
        }
        assert_eq!(count as u32, m * n);
    }

    // reverse = true
    {
        let builder = MergeIterOverBuilder::default()
            .reverse(true)
            .add(Box::new(UniIterator::new(st1, true)))
            .add(Box::new(UniIterator::new(st2, true)))
            .add(Box::new(UniIterator::new(st3, true)));
        let miter = builder.build();
        assert!(miter.peek().is_none());
        let mut count = 0;
        keys.lock().await.sort_by(|a, b| b.cmp(a));
        while let Some(value) = miter.next() {
            // info!("{}", String::from_utf8_lossy(value.key()));
            let expect = keys.lock().await;
            let expect = expect.get(count).unwrap();
            assert_eq!(value.key(), expect);
            count += 1;
        }
        assert_eq!(count as u32, m * n);
    }
}

#[tokio::test]
async fn merge_iter_random() {
    use itertools::Itertools;
    crate::test_util::tracing_log();
    let st1 = SkipList::new(1 << 20);
    let st2 = SkipList::new(1 << 20);
    let st3 = SkipList::new(1 << 20);
    let mut keys = vec![];
    for i in 0..10000 {
        let key = rand::random::<usize>() % 10000;
        let key = format!("k{:05}", key).into_bytes().to_vec();
        keys.push(key.clone());
        if i % 3 == 0 {
            st1.put(&key, ValueStruct::new(vec![1, 23], 0, 9, 0))
        } else if i % 3 == 1 {
            st2.put(&key, ValueStruct::new(vec![1, 23], 0, 9, 0))
        } else {
            st3.put(&key, ValueStruct::new(vec![1, 23], 0, 9, 0))
        }
    }

    {
        let builder = MergeIterOverBuilder::default()
            .add(Box::new(UniIterator::new(st1.clone(), false)))
            .add(Box::new(UniIterator::new(st2.clone(), false)))
            .add(Box::new(UniIterator::new(st3.clone(), false)));
        keys.sort();
        let keys = keys.clone().into_iter().unique().collect::<Vec<_>>();
        let miter = builder.build();
        assert!(miter.peek().is_none());
        let mut count = 0;
        while let Some(value) = miter.next() {
            // info!("{}", String::from_utf8_lossy(value.key()));
            let expect = keys.get(count).unwrap();
            assert_eq!(value.key(), expect);
            count += 1;
        }
        assert_eq!(count, keys.len());
    }

    {
        let builder = MergeIterOverBuilder::default()
            .reverse(true)
            .add(Box::new(UniIterator::new(st1.clone(), true)))
            .add(Box::new(UniIterator::new(st2.clone(), true)))
            .add(Box::new(UniIterator::new(st3.clone(), true)));
        keys.sort_by(|a, b| b.cmp(a));
        let keys = keys.into_iter().unique().collect::<Vec<_>>();
        let miter = builder.build();
        assert!(miter.peek().is_none());
        let mut count = 0;
        while let Some(value) = miter.next() {
            // info!("{}", String::from_utf8_lossy(value.key()));
            let expect = keys.get(count).unwrap();
            assert_eq!(value.key(), expect);
            count += 1;
        }
        assert_eq!(count, keys.len());
    }
}
