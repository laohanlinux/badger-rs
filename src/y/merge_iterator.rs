use log::info;

use crate::hex_str;
use crate::table::iterator::{IteratorImpl, IteratorItem};

use crate::y::iterator::Xiterator;
use crate::y::KeyValue;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::btree_set::Iter;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::os::macos::raw::stat;

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
    pub heap: RefCell<BinaryHeap<IterRef>>,
    pub heap_flag: RefCell<Vec<bool>>,
}

impl Xiterator for MergeIterator {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.itrs.is_empty() {
            return None;
        }
        assert_ne!(self.cursor.borrow().index, usize::MAX);
        self._next()
    }

    // fn next(&self) -> Option<Self::Output> {
    //     if self.itrs.is_empty() {
    //         return None;
    //     }
    //     assert_ne!(self.cursor.borrow().index, usize::MAX);
    //
    //     // if self.cursor.borrow().index == usize::MAX {
    //     //     for itr in &self.itrs {
    //     //         itr.rewind();
    //     //     }
    //     //     self.cursor.borrow_mut().index = 0;
    //     // }
    //     let mut latest: Option<IteratorItem> = None;
    //     let mut index = usize::MAX;
    //     // Use stack memory
    //     let mut same_iterators = Vec::with_capacity(self.itrs.len());
    //     #[cfg(test)]
    //     let mut stack = vec![];
    //     for (itr_index, itr) in self.itrs.iter().enumerate() {
    //         if let Some(item) = itr.peek() {
    //             #[cfg(test)]
    //             stack.push(item.key.clone());
    //
    //             if let Some(have_latest) = &mut latest {
    //                 if !self.reverse {
    //                     match item.key().cmp(have_latest.key()) {
    //                         std::cmp::Ordering::Less => {
    //                             index = itr_index;
    //                             *have_latest = item;
    //                             same_iterators.push(itr_index);
    //                         }
    //                         std::cmp::Ordering::Equal => {
    //                             same_iterators.push(itr_index);
    //                         }
    //                         std::cmp::Ordering::Greater => {}
    //                     }
    //                 } else {
    //                     match item.key().cmp(have_latest.key()) {
    //                         std::cmp::Ordering::Less => {}
    //                         std::cmp::Ordering::Equal => {
    //                             same_iterators.push(itr_index);
    //                         }
    //                         std::cmp::Ordering::Greater => {
    //                             index = itr_index;
    //                             *have_latest = item;
    //                             same_iterators.push(itr_index);
    //                         }
    //                     }
    //                 }
    //             } else {
    //                 latest.replace(item);
    //                 index = itr_index;
    //                 same_iterators.push(itr_index);
    //             }
    //         }
    //     }
    //
    //     if index != usize::MAX {
    //         assert!(latest.is_some());
    //
    //         #[cfg(test)]
    //         {
    //             let str = stack.into_iter().map(|key| hex_str(&key)).join(",");
    //             let buf = format!(
    //                 "found target: {}, {}, iter_count {}",
    //                 hex_str(latest.as_ref().unwrap().key()),
    //                 str,
    //                 self.itrs.len()
    //             );
    //             crate::test_util::push_log(buf.as_bytes(), false);
    //         }
    //
    //         // Move same iterators
    //         #[cfg(test)]
    //         info!("--------------------------");
    //         for (itr_index) in same_iterators.iter().rev() {
    //             if let Some(key) = self.itrs[*itr_index].peek() && key.key() == latest.as_ref().unwrap().key() {
    //                 #[cfg(test)]
    //                 info!("Move key #{}, index {}, {}, meta:{}, {}", hex_str(key.key()), itr_index, index, latest.as_ref().unwrap().value().meta, key.value().meta);
    //                 if *itr_index != index {
    //                     self.export_disk();
    //                 }
    //                  self.itrs[*itr_index].next();
    //                 assert_eq!(*itr_index, index);
    //             } else {
    //                 break;
    //             }
    //         }
    //         #[cfg(test)]
    //         info!("--------------------------");
    //         self.cursor.borrow_mut().replace(index, latest);
    //         //self.itrs.get(index).as_ref().unwrap().next(); // Skip current node
    //     } else {
    //         self.cursor.borrow_mut().index = 0;
    //         self.cursor.borrow_mut().cur_item.take(); // Not found any thing.
    //     }
    //     self.peek()
    // }

    /// Rewind iterator and return the current item (So it call next that will move to second item).
    /// Notice: 1: Only rewind inner iterators list. Not reverse self.itrs vec sequence, 2: maybe some iterator is empty (Why, TODO)
    /// TODO: Opz rewind algo
    //    fn rewind(&self) -> Option<Self::Output> {
    //      if self.itrs.is_empty() {
    //        return None;
    //   }
    //  let mut heap = self.heap.borrow_mut();
    // for (index, itr) in self.itrs.iter().enumerate() {
    //    if let Some(item) = itr.rewind() {
    ///      heap.push(IterRef { index, key: item })
    // }
    // }
    ///self.cursor.borrow_mut().index = 0;
    // self.next()
    //}

    fn rewind(&self) -> Option<Self::Output> {
        if self.itrs.is_empty() {
            self.set_iter_empty();
            return None;
        }
        {
            self.heap.borrow_mut().clear();
            for (index, itr) in self.itrs.iter().enumerate() {
                if let Some(item) = itr.rewind() {
                    // info!("rewind {}, {}", index, hex_str(item.key()));
                    self.push_item_into_heap(index, item);
                }
            }
            self.set_iter_empty();
        }

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
            #[cfg(test)]
            crate::test_util::push_log(buffer.as_bytes(), false);
        }
    }
    fn _next(&self) -> Option<IteratorItem> {
        let mut stack = vec![];
        {
            {
                let heap = self.heap.borrow_mut();
                if heap.is_empty() {
                    self.set_iter_empty();
                    return None;
                }
            }
            let first_el = self.pop_item_from_heap();
            if first_el.is_none() {
                self.set_iter_empty();
                return None;
            }
            let first_el = first_el.unwrap();
            // Move it
            self.cursor.borrow_mut().index = first_el.index;
            self.cursor
                .borrow_mut()
                .cur_item
                .replace(first_el.key.clone());
            self.itrs.get(first_el.index).unwrap().next();
            stack.push(first_el.index);

            // move dummp keys
            loop {
                let mut heap = self.heap.borrow_mut();
                let pop = heap.peek();
                if pop.is_none() {
                    break;
                }
                let pop = pop.unwrap();
                let pop_key = pop.key.key();
                let first_key = first_el.key.key();
                let index = pop.index;
                if pop_key != first_key {
                    break;
                }
                // Find the same, pop it
                heap.pop();
                stack.push(index);
                self.itrs.get(index).unwrap().next();
            }
        }

        self.init_heap_by_indexs(stack);
        // self.init_heap();
        self.peek()
    }

    fn set_iter_empty(&self) {
        self.cursor.borrow_mut().index = 0;
        self.cursor.borrow_mut().cur_item.take();
    }

    fn init_heap(&self) {
        // self.heap.borrow_mut().clear();
        // Only push has element
        for (index, itr) in self.itrs.iter().enumerate() {
            if let Some(item) = itr.peek() {
                self.push_item_into_heap(index, item);
            }
        }
    }

    fn init_heap_by_indexs(&self, stack: Vec<usize>) {
        for index in stack {
            if let Some(item) = self.itrs[index].peek() {
                self.push_item_into_heap(index, item);
            }
        }
    }

    #[inline]
    fn push_item_into_heap(&self, index: usize, item: IteratorItem) {
        if self.heap_flag.borrow()[index] {
            return;
        }

        self.heap_flag.borrow_mut()[index] = true;
        self.heap.borrow_mut().push(IterRef {
            index,
            key: item,
            rev: self.reverse,
        });
    }

    #[inline]
    fn pop_item_from_heap(&self) -> Option<IterRef> {
        if let Some(item) = self.heap.borrow_mut().pop() {
            self.heap_flag.borrow_mut()[item.index] = false;
            return Some(item);
        }
        None
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
        let cap = self.all.len();
        let mut flag = vec![false; cap];
        MergeIterator {
            reverse: self.reverse,
            itrs: self.all,
            cursor: RefCell::new(MergeCursor {
                index: usize::MAX,
                cur_item: None,
            }),
            heap_flag: RefCell::new(flag),
            heap: RefCell::new(BinaryHeap::with_capacity(cap)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IterRef {
    index: usize,
    key: IteratorItem,
    rev: bool,
}

impl Ord for IterRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if !self.rev {
            if self.key.key() == other.key.key() {
                return other.index.cmp(&self.index);
            }
            other.key.key().cmp(&self.key.key())
        } else {
            if self.key.key() == other.key.key() {
                return other.index.cmp(&self.index);
            }
            self.key.key().cmp(&other.key.key())
        }
    }
}

impl PartialOrd for IterRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for IterRef {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.key.key() == other.key.key()
    }
}

impl Eq for IterRef {}

#[cfg(test)]
mod tests {
    use std::fmt;
    use tracing::info;

    use crate::{
        hex_str, table::iterator::IteratorItem, types::TArcMx, KeyValue, MergeIterOverBuilder,
        SkipList, UniIterator, ValueStruct, Xiterator,
    };

    use super::IterRef;

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
            miter.rewind();
            let mut count = 1;
            while let Some(value) = miter.next() {
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
            miter.rewind();
            let mut count = 1;
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
            let key = miter.rewind();
            let mut count = 1;
            assert_eq!(hex_str(key.as_ref().unwrap().key()), hex_str(&keys[0]));
            while let Some(value) = miter.next() {
                // log::info!("{}", String::from_utf8_lossy(value.key()));
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
            let mut count = 1;
            miter.rewind();
            while let Some(value) = miter.next() {
                let expect = keys.get(count).unwrap();
                assert_eq!(value.key(), expect);
                count += 1;
            }
            assert_eq!(count, keys.len());
        }
    }

    #[tokio::test]
    async fn heap_iter() {
        let build_key = |key: Vec<u8>| -> IteratorItem {
            IteratorItem {
                key,
                value: ValueStruct::default(),
            }
        };
        let elements = vec![
            IterRef {
                index: 10,
                key: build_key(b"40".to_vec()),
                rev: false,
            },
            IterRef {
                index: 20,
                key: build_key(b"59".to_vec()),
                rev: false,
            },
            IterRef {
                index: 9,
                key: build_key(b"40".to_vec()),
                rev: false,
            },
            IterRef {
                index: 7,
                key: build_key(b"41".to_vec()),
                rev: false,
            },
        ];

        let mut heap = std::collections::BinaryHeap::new();
        for el in elements {
            heap.push(std::cmp::Reverse(el));
        }
        crate::test_util::tracing_log();
        let first = heap.pop().unwrap();
        log::info!("{:?}", first);
    }
}
