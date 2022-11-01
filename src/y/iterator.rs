use crate::options::FileLoadingMode;
use crate::skl::BlockBytes;
use crate::skl::Chunk;
use crate::table::iterator::{BlockIteratorItem, IteratorImpl, IteratorItem};
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde_json::Value;
use std::borrow::{Borrow, Cow};
use std::cell::{Cell, RefCell, RefMut};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt;
use std::fmt::Formatter;
use std::io::{Cursor, Read, Write};
use std::iter::Iterator as stdIterator;
use std::marker::PhantomData;
use std::mem::size_of;
use std::process::id;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::rc::Rc;
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};
use std::sync::Arc;

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value_size|value|
#[derive(Debug, Clone, Default, PartialEq)]
#[repr(C)]
pub struct ValueStruct {
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) cas_counter: u64,
    pub(crate) value: Vec<u8>,
}

impl ValueStruct {
    pub(crate) fn new(value: Vec<u8>, meta: u8, user_meta: u8, cas_counter: u64) -> ValueStruct {
        ValueStruct {
            meta,
            user_meta,
            cas_counter,
            value,
        }
    }
    pub(crate) const fn header_size() -> usize {
        10
    }

    fn size(&self) -> usize {
        Self::header_size() + self.value.len()
    }

    pub(crate) fn write_data(&self, mut buffer: &mut [u8]) {
        use std::io::Write;
        let mut cursor = Cursor::new(buffer);
        cursor.write_u8(self.meta).unwrap();
        cursor.write_u8(self.user_meta).unwrap();
        cursor.write_u64::<BigEndian>(self.cas_counter).unwrap();
        cursor.write_all(&self.value).unwrap();
    }

    pub(crate) fn read_data(&mut self, buffer: &[u8]) {
        use std::io::Read;
        let mut cursor = Cursor::new(buffer);
        self.meta = cursor.read_u8().unwrap();
        self.user_meta = cursor.read_u8().unwrap();
        self.cas_counter = cursor.read_u64::<BigEndian>().unwrap();
        self.value.extend_from_slice(&buffer[Self::header_size()..]);
    }
}

impl ValueStruct {
    #[inline]
    pub(crate) fn get_data_mut_ptr(&self) -> *mut u8 {
        self as *const Self as *const u8 as *mut u8
    }
}

impl<T> From<T> for ValueStruct
where
    T: AsRef<[u8]>,
{
    fn from(buffer: T) -> Self {
        let mut v = ValueStruct::default();
        v.read_data(buffer.as_ref());
        v
    }
}

impl Into<Vec<u8>> for &ValueStruct {
    fn into(self) -> Vec<u8> {
        let mut buffer = vec![0; self.size()];
        self.write_data(&mut buffer);
        buffer
    }
}

pub trait Xiterator {
    type Output;
    fn next(&self) -> Option<Self::Output>;
    fn rewind(&self) -> Option<Self::Output>;
    fn seek(&self, key: &[u8]) -> Option<Self::Output>;
    fn peek(&self) -> Option<Self::Output> {
        todo!()
    }
    fn prev(&self) -> Option<Self::Output> {
        todo!()
    }
}

pub trait KeyValue<V> {
    fn key(&self) -> &[u8];
    fn value(&self) -> V;
}

#[derive(Default)]
pub struct MergeIterOverIterator<'a> {
    cur_key: RefCell<Vec<u8>>,
    reverse: bool,
    pub(crate) all: Vec<IterOverXIterator<'a>>,
    pub(crate) elements: RefCell<Vec<IterOverXIterator<'a>>>,
}

impl Xiterator for MergeIterOverIterator<'_> {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.elements.borrow().is_empty() {
            return None;
        }

        let mut keyvalue: Option<IteratorItem> = None;
        for (idx, tb_iter) in self.elements.borrow().iter().enumerate() {
            let item = tb_iter.m.peek();
            if item.is_none() {
                continue;
            }
            println!("preview");
            let item = item.unwrap();

            if keyvalue.is_none() {
                keyvalue = Some(item.clone());
            }

            // continue proboe
            if keyvalue.as_ref().unwrap().key() == item.key() {
                tb_iter.m.next(); // Move
                continue;
            }
            break;
        }
        self.reset();
        if let Some(ref item) = keyvalue {
            self.store_key(item.key());
        }
        keyvalue
    }

    fn rewind(&self) -> Option<Self::Output> {
        for itr in self.all.iter() {
            itr.m.rewind();
        }
        self.reset();
        self.peek()
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        for iter in self.all.iter() {
            iter.m.seek(key);
        }
        self.reset();
        self.peek()
    }

    fn peek(&self) -> Option<Self::Output> {
        if let Some(itr) = self.elements.borrow().first() {
            return itr.m.peek();
        }
        None
    }
}

impl MergeIterOverIterator<'_> {
    // todo: reset all iterator index?
    fn reset(&self) {
        let elements = self
            .all
            .iter()
            .filter(|x| x.m.peek().is_some())
            .map(|x| x.m.get_iter())
            .collect::<Vec<_>>();

        println!("new elements: {}, {}", self.all.len(), elements.len());
        *self.elements.borrow_mut() = elements;
        self.elements.borrow_mut().sort();
        if let Some(cur_itr) = self.elements.borrow().first() {
            if let Some(cur_key) = cur_itr.m.peek() {
                self.store_key(cur_key.key());
            } else {
                self.store_key(b"");
            }
        }
    }

    fn store_key(&self, key: &[u8]) {
        self.cur_key.borrow_mut().clear();
        self.cur_key.borrow_mut().extend_from_slice(key);
        println!(
            "new cur key: {}",
            String::from_utf8_lossy(self.cur_key.borrow().as_slice())
        );
    }
}

#[derive(Default)]
pub struct MergeIterOverBuilder<'a> {
    all: Vec<&'a dyn Xiterator<Output = IteratorItem>>,
    reverse: bool,
}

impl<'a> MergeIterOverBuilder<'a> {
    pub fn add(mut self, x: &'a dyn Xiterator<Output = IteratorItem>) -> MergeIterOverBuilder<'_> {
        self.all.push(x);
        self
    }

    pub fn add_batch(
        mut self,
        iters: Vec<&'a dyn Xiterator<Output = IteratorItem>>,
    ) -> MergeIterOverBuilder {
        self.all.extend_from_slice(&iters);
        self
    }

    pub fn build(mut self) -> MergeIterOverIterator<'a> {
        let mut all = vec![];
        for (index, e) in self.all.iter().enumerate() {
            e.next(); // Note: move it to first item, very import, maybe peek check
            let mut itr = e.get_iter();
            itr.nice = index as isize + 1;
            all.push(itr);
        }
        let m = MergeIterOverIterator {
            all,
            reverse: self.reverse,
            ..Default::default()
        };
        // do some
        m.reset();
        m
    }
}

impl<'a> MergeIterOverIterator<'a> {
    fn new(all: Vec<IterOverXIterator<'a>>, elements: Vec<IterOverXIterator<'a>>) -> Self {
        MergeIterOverIterator {
            all,
            elements: RefCell::new(elements),
            ..Default::default()
        }
    }
}

pub struct IterOverXIterator<'a> {
    nice: isize,
    pub(crate) m: &'a dyn Xiterator<Output = IteratorItem>,
}

impl PartialEq<Self> for IterOverXIterator<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.m.peek().unwrap().key() == other.m.peek().unwrap().key()
    }
}

impl PartialOrd<Self> for IterOverXIterator<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.m.peek().unwrap().key() == other.m.peek().unwrap().key() {
            return Some(self.nice.cmp(&other.nice));
        }
        Some(
            self.m
                .peek()
                .unwrap()
                .key()
                .cmp(other.m.peek().unwrap().key()),
        )
    }
}

impl Eq for IterOverXIterator<'_> {}

impl Ord for IterOverXIterator<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.nice.cmp(&other.nice)
    }
}

impl<'a> IterOverXIterator<'a> {
    fn new(m: &'a dyn Xiterator<Output = IteratorItem>) -> Self {
        Self { m, nice: 0 }
    }
}

impl dyn Xiterator<Output = IteratorItem> + '_ {
    fn get_iter<'a>(&'a self) -> IterOverXIterator<'a> {
        IterOverXIterator::new(self)
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

    impl super::iterator::Xiterator for TestIterator {
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

    let t1 = TestIterator {
        key: b"abd".to_vec(),
    };
    let t2 = TestIterator {
        key: b"abc".to_vec(),
    };
    let t3 = TestIterator {
        key: b"abc".to_vec(),
    };
    let t4 = TestIterator {
        key: b"abc".to_vec(),
    };

    let builder = MergeIterOverBuilder::default().add_batch(vec![&t3, &t1, &t4, &t2]);
    let miter = builder.build();
    miter.elements.borrow_mut().sort();
    miter.elements.borrow().iter().for_each(|e| {
        println!(
            "{}, {:?}",
            e.nice,
            String::from_utf8_lossy(e.m.peek().unwrap().key())
        );
    });
}

#[test]
fn merge_iter_e() {}
