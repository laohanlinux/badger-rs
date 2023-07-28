use log::info;

use crate::hex_str;
use crate::table::iterator::{IteratorImpl, IteratorItem};

use crate::y::iterator::Xiterator;
use crate::y::KeyValue;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::HashMap;

/// Cursor of the iterator of merge.
pub struct MergeCursor {
    // At init index is set to MAX
    pub index: usize,
    pub cur_item: Option<IteratorItem>,
}

impl MergeCursor {
    fn replace(&mut self, index: usize, cur_item: Option<IteratorItem>) {
        self.index = index;
        self.cur_item = cur_item;
    }
}

/// A iterator for multi iterator merge into one.
pub struct MergeIterator {
    pub reverse: bool,
    pub itrs: Vec<Box<dyn Xiterator<Output = IteratorItem>>>,
    pub cursor: RefCell<MergeCursor>,
}

impl Xiterator for MergeIterator {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.itrs.is_empty() {
            return None;
        }
        assert_ne!(self.cursor.borrow().index, usize::MAX);
        // if self.cursor.borrow().index == usize::MAX {
        //     for itr in &self.itrs {
        //         itr.rewind();
        //     }
        //     self.cursor.borrow_mut().index = 0;
        // }
        let mut latest: Option<IteratorItem> = None;
        let mut index = usize::MAX;
        // Use stack memory
        let mut same_iterators = Vec::with_capacity(self.itrs.len());
        #[cfg(test)]
        let mut stack = vec![];
        for (itr_index, itr) in self.itrs.iter().enumerate() {
            if let Some(item) = itr.peek() {
                #[cfg(test)]
                stack.push(item.key.clone());

                if let Some(have_latest) = &mut latest {
                    if !self.reverse {
                        match item.key().cmp(have_latest.key()) {
                            std::cmp::Ordering::Less => {
                                index = itr_index;
                                *have_latest = item;
                                same_iterators.push(itr_index);
                            }
                            std::cmp::Ordering::Equal => {
                                same_iterators.push(itr_index);
                            }
                            std::cmp::Ordering::Greater => {}
                        }
                    } else {
                        match item.key().cmp(have_latest.key()) {
                            std::cmp::Ordering::Less => {}
                            std::cmp::Ordering::Equal => {
                                same_iterators.push(itr_index);
                            }
                            std::cmp::Ordering::Greater => {
                                index = itr_index;
                                *have_latest = item;
                                same_iterators.push(itr_index);
                            }
                        }
                    }
                } else {
                    latest.replace(item);
                    index = itr_index;
                    same_iterators.push(itr_index);
                }
            }
        }

        if index != usize::MAX {
            assert!(latest.is_some());

            #[cfg(test)]
            {
                let str = stack.into_iter().map(|key| hex_str(&key)).join(",");
                let buf = format!(
                    "found target: {}, {}, iter_count {}",
                    hex_str(latest.as_ref().unwrap().key()),
                    str,
                    self.itrs.len()
                );
                crate::test_util::push_log(buf.as_bytes(), false);
            }

            // Move same iterators
            #[cfg(test)]
            info!("--------------------------");
            for (itr_index) in same_iterators.iter().rev() {
                if let Some(key) = self.itrs[*itr_index].peek() && key.key() == latest.as_ref().unwrap().key() {
                    #[cfg(test)]
                    info!("Move key #{}, index {}, {}, meta:{}, {}", hex_str(key.key()), itr_index, index, latest.as_ref().unwrap().value().meta, key.value().meta);
                    if *itr_index != index {
                        self.export_disk();
                    }
                     self.itrs[*itr_index].next();
                    assert_eq!(*itr_index, index);
                } else {
                    break;
                }
            }
            #[cfg(test)]
            info!("--------------------------");
            self.cursor.borrow_mut().replace(index, latest);
            //self.itrs.get(index).as_ref().unwrap().next(); // Skip current node
        } else {
            self.cursor.borrow_mut().index = 0;
            self.cursor.borrow_mut().cur_item.take(); // Not found any thing.
        }
        self.peek()
    }

    /// Rewind iterator and return the current item (So it call next that will move to second item).
    /// Notice: 1: Only rewind inner iterators list. Not reverse self.itrs vec sequence, 2: maybe some iterator is empty (Why, TODO)
    /// TODO: Opz rewind algo
    fn rewind(&self) -> Option<Self::Output> {
        if self.itrs.is_empty() {
            return None;
        }
        for itr in self.itrs.iter() {
            itr.rewind();
        }
        self.cursor.borrow_mut().index = 0;
        self.next()
    }

    fn seek(&self, _key: &[u8]) -> Option<Self::Output> {
        todo!()
    }

    // TODO avoid clone
    fn peek(&self) -> Option<Self::Output> {
        self.cursor.borrow().cur_item.clone()
    }

    fn id(&self) -> String {
        format!("{}", self.cursor.borrow().index)
    }
}

impl std::iter::Iterator for MergeIterator {
    type Item = IteratorItem;

    fn next(&mut self) -> Option<Self::Item> {
        Xiterator::next(self)
    }
}

impl MergeIterator {
    #[inline]
    fn count(&self) -> usize {
        let mut count = 0;
        for itr in &self.itrs {
            itr.rewind();
        }
        for (_index, itr) in self.itrs.iter().enumerate() {
            while itr.next().is_some() {
                count += 1;
            }
        }
        count
    }

    fn export_disk(&self) {
        for itr in self.itrs.iter() {
            let mut keys = vec![];
            if let Some(key) = itr.peek() {
                keys.push(hex_str(key.key()));
            }
            let buffer = keys.join("#");
            crate::test_util::push_log(buffer.as_bytes(), false);
        }
    }
}

/// A Builder for merge iterator building
#[derive(Default)]
pub struct MergeIterOverBuilder {
    all: Vec<Box<dyn Xiterator<Output = IteratorItem>>>,
    reverse: bool,
}

impl MergeIterOverBuilder {
    pub fn reverse(mut self, reverse: bool) -> MergeIterOverBuilder {
        self.reverse = reverse;
        self
    }

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

    pub fn build(self) -> MergeIterator {
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

#[cfg(test)]
mod tests {
    use std::fmt;

    use crate::{
        table::iterator::IteratorItem, types::TArcMx, KeyValue, MergeIterOverBuilder, SkipList,
        UniIterator, ValueStruct, Xiterator,
    };

    #[test]
    fn merge_iter_element() {
        #[derive(Debug)]
        struct TestIterator {
            key: Vec<u8>,
        }

        impl fmt::Display for TestIterator {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
}
