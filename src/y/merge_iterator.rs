use log::{debug, info, warn};

use crate::hex_str;
use crate::table::iterator::{IteratorImpl, IteratorItem};

use crate::kv::{_BADGER_PREFIX, _HEAD};
use crate::y::iterator::Xiterator;
use crate::y::KeyValue;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::btree_set::Iter;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt::format;
use tracing::error;

/// Cursor of the iterator of merge.
pub struct MergeCursor {
    // At init index is set to MAX
    pub index: usize,
    pub cur_item: Option<IteratorItem>,
}

impl MergeCursor {
    fn replace(&mut self, index: usize, cur_item: Option<IteratorItem>) {
        if cur_item.is_some() {
            assert!(!cur_item.as_ref().unwrap().key().starts_with(_BADGER_PREFIX));
        }
        self.index = index;
        self.cur_item = cur_item;
    }

    fn get_item(&self) -> Option<&IteratorItem> {
        return self.cur_item.as_ref();
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
            self.set_iter_empty();
            return None;
        }
        assert_ne!(self.cursor.borrow().index, usize::MAX);
        self._next()
    }

    fn rewind(&self) -> Option<Self::Output> {
        if self.itrs.is_empty() {
            self.set_iter_empty();
            warn!("no found any iterator!");
            return None;
        }
        {
            // Before every rewind, all flags will be resetted
            self.reset();
            for (index, itr) in self.itrs.iter().enumerate() {
                if itr.rewind().is_some() {
                    while let Some(item) = itr.peek() {
                        if item.key().starts_with(_BADGER_PREFIX) {
                            itr.next();
                            continue;
                        }
                        self.push_item_into_heap(index, item);
                        break;
                    }
                } else {
                    warn!("has a empty iterator, index:{}, id:{}", index, itr.id());
                }
            }
            self.set_iter_empty();
        }

        self.next()
    }

    fn seek(&self, _key: &[u8]) -> Option<Self::Output> {
        self.reset();
        for (index, itr) in self.itrs.iter().enumerate() {
            if let Some(item) = itr.seek(_key) {
                self.push_item_into_heap(index, item);
            }
        }
        self.set_iter_empty();
        self.next()
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

    fn reset(&self) {
        self.cursor.borrow_mut().replace(usize::MAX, None);
        self.heap.borrow_mut().clear();
        self.heap_flag
            .borrow_mut()
            .iter_mut()
            .for_each(|flag| *flag = false);
    }

    pub(crate) fn export_disk(&self) {
        #[cfg(test)]
        for itr in self.itrs.iter() {
            let mut keys = vec![];
            while let Some(key) = itr.peek() {
                //keys.push(format!("{},{}", itr.id(), hex_str(key.key())));
                keys.push(format!("{}", hex_str(key.key())));
                itr.next();
            }
            let buffer = keys.join("#");
            crate::test_util::push_log_by_filename("merge_iterator.txt", buffer.as_bytes());
        }
    }

    pub(crate) fn export_disk_ext(&self) {
        #[cfg(test)]
        {
            for itr in self.itrs.iter() {
                let mut keys = vec![];
                let mut has = HashMap::new();
                while let Some(item) = itr.peek() {
                    keys.push(item.clone());
                    itr.next();
                    if let Some(_old) = has.insert(item.key.clone(), item.clone()) {
                        panic!("it should be not happen, dump key: {}", hex_str(item.key()));
                    }
                }
                let buffer = serde_json::to_vec(&keys).unwrap();
                crate::test_util::push_log_by_filename("test_data/merge_iterator_ext.txt", &buffer);
            }
        }
    }

    fn _next(&self) -> Option<IteratorItem> {
        // the stack store the index that should be repush to heap
        let mut stack = vec![];
        {
            {
                let heap = self.heap.borrow_mut();
                if heap.is_empty() {
                    self.set_iter_empty();
                    return None;
                }
            }
            // Pop the first element
            let first_el = self.pop_item_from_heap();
            if first_el.is_none() {
                self.set_iter_empty();
                return None;
            }
            let first_el = first_el.unwrap();
            self.cursor
                .borrow_mut()
                .replace(first_el.index, Some(first_el.key.clone()));
            // Move the iterator
            self.itrs.get(first_el.index).unwrap().next();
            stack.push(first_el.index);

            #[cfg(test)]
            debug!(
                "Find the target, key: {}, index: {}",
                hex_str(self.cursor.borrow().get_item().unwrap().key()),
                first_el.index
            );
            // move dump keys
            loop {
                let heap = self.heap.borrow_mut();
                let peek = heap.peek();
                if peek.is_none() {
                    break;
                }
                let pop = peek.unwrap();
                let pop_key = pop.key.key();
                let first_key = first_el.key.key();
                let index = pop.index;
                // the key not equal the next key.
                if pop_key != first_key {
                    break;
                }
                // Find the same, pop it
                #[cfg(test)]
                info!("Find a same value, {}", hex_str(pop_key));
                drop(heap);
                self.pop_item_from_heap();

                stack.push(index);
                // Move same key iterator
                self.itrs.get(index).unwrap().next();
            }
        }

        self.init_heap_by_indexes(stack);
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

    fn init_heap_by_indexes(&self, stack: Vec<usize>) {
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
        let flag = vec![false; cap];
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
                // NOTICE that
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
    use log::{error, warn};
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

        for _ in 0..100 {
            let st1 = SkipList::new(1 << 20);
            let st2 = SkipList::new(1 << 20);
            let st3 = SkipList::new(1 << 20);
            let mut keys = vec![];
            for i in 0..10000 {
                let key = rand::random::<usize>() % 10000;
                let key = format!("k{:05}", key).into_bytes().to_vec();
                keys.push(key.clone());
                if (i % 3) == 0 {
                    st1.put(&key, ValueStruct::new(vec![1, 23], 0, 9, 0))
                } else if (i % 3) == 1 {
                    st2.put(&key, ValueStruct::new(vec![1, 23], 0, 9, 0))
                } else {
                    st3.put(&key, ValueStruct::new(vec![1, 23], 0, 9, 0))
                }
            }
            keys.sort();
            let mut keys = keys.clone().into_iter().unique().collect::<Vec<_>>();
            let pretty_st = |st: SkipList| {
                let mut keys = vec![];
                let cur = st.new_cursor();
                while let Some(item) = cur.next() {
                    let key = item.key(&st.arena);
                    keys.push(key);
                }
                info!(
                    "{}",
                    keys[0..10].into_iter().map(|key| hex_str(key)).join(",")
                );
            };
            {
                pretty_st(st1.clone());
                pretty_st(st2.clone());
                pretty_st(st3.clone());
                info!(
                    "sort keys: {}",
                    keys[0..10].iter().map(|key| hex_str(key)).join(",")
                );
            }
            {
                let builder = MergeIterOverBuilder::default()
                    .add(Box::new(UniIterator::new(st1.clone(), false)))
                    .add(Box::new(UniIterator::new(st2.clone(), false)))
                    .add(Box::new(UniIterator::new(st3.clone(), false)));
                let miter = builder.build();
                assert!(miter.peek().is_none());
                let key = miter.rewind();
                let mut count = 1;
                assert_eq!(hex_str(key.as_ref().unwrap().key()), hex_str(&keys[0]));
                while let Some(value) = miter.next() {
                    let expect = keys.get(count).unwrap();
                    assert_eq!(
                        value.key(),
                        expect,
                        "{} not equal {}",
                        hex_str(value.key()),
                        hex_str(expect)
                    );
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
