use crate::skl::BlockBytes;
use crate::skl::Chunk;
use crate::table::iterator::IteratorItem;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde_json::Value;
use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::io::{Cursor, Read, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value_size|value|
#[derive(Debug, Default, PartialEq)]
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
        // println!("{:?}", buffer);
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
        // println!("{:?}", buffer);
        buffer
    }
}

pub trait Iterator {
    type Output;
    fn next(&self) -> Option<Self::Output>;
    fn rewind(&self) -> Option<Self::Output>;
    fn seek(&self, key: &[u8]) -> Option<Self::Output>;
    fn valid(&self) -> bool;
    fn close(&self);
}

pub trait KeyValue<V> {
    fn key(&self) -> &[u8];
    fn value(&self) -> V;
}
// index: RefCell<isize>,      // Which iterator is active now. todo use usize
// iters: Vec<Iterator<'a>>,   // Corresponds to `tables`.
// tables: Vec<&'a TableCore>, // Disregarding `reversed`, this is in ascending order.
// reversed: bool,

pub struct MergeIterator<'a> {
    // heap: BinaryHeap<MergeIteratorElement<crate::table::iterator::Iterator<'a>>>,
    cur_key: Vec<u8>,
    reverse: bool,
    all: Vec<crate::table::iterator::Iterator<'a>>,
    elements: Vec<MergeIteratorElement<crate::table::iterator::Iterator<'a>>>,
    mbos: RefCell<isize>,
}

impl<'a> MergeIterator<'a> {
    pub fn store_key(&mut self, key: &[u8]) {
        self.cur_key.clear();
        self.cur_key.extend_from_slice(key);
    }
    // initHeap checks all iterators and initializes our heap and array of keys.
    // Whenever we reverse direction, we need to run this.
    fn init(&mut self) {}

    fn get_or_set_iter(&self, mbos: isize) -> &crate::table::iterator::Iterator<'_> {
        // if self.mbos.borrow()
        todo!()
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        todo!()
    }

    fn rewind(&self) -> Option<Self::Output> {
        todo!()
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        let mut first: Option<IteratorItem> = None;
        for iter in self.all.iter() {
            if let Some(item) = iter.seek(key) {
                if first.is_none() || first.as_ref().unwrap().key() > item.key() {
                    first = Some(item);
                }
            }
        }
        // TODO: init
        first
    }

    fn valid(&self) -> bool {
        todo!()
    }

    fn close(&self) {
        todo!()
    }
}

#[derive(Debug)]
pub struct MergeIteratorElement<I>
where
    I: Iterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    nice: isize,
    itr: I,
    reverse: bool,
}

impl<I> PartialEq<Self> for MergeIteratorElement<I>
where
    I: Iterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    fn eq(&self, other: &Self) -> bool {
        self.itr.key() == other.itr.key()
    }
}

impl<I> PartialOrd<Self> for MergeIteratorElement<I>
where
    I: Iterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.itr.key() == other.itr.key() {
            return Some(self.nice.cmp(&other.nice));
        }
        Some(self.itr.key().cmp(other.itr.key()))
    }
}

impl<I> Eq for MergeIteratorElement<I> where
    I: Iterator<Output = IteratorItem> + KeyValue<ValueStruct>
{
}

impl<I> Ord for MergeIteratorElement<I>
where
    I: Iterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.nice.cmp(&self.nice)
    }
}

#[test]
fn merge_iter_element() {
    #[derive(Debug)]
    struct TestIterator {
        key: Vec<u8>,
    }

    impl super::iterator::Iterator for TestIterator {
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

        fn valid(&self) -> bool {
            todo!()
        }

        fn close(&self) {
            todo!()
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

    let e1 = MergeIteratorElement {
        nice: 1,
        itr: TestIterator {
            key: b"abd".to_vec(),
        },
        reverse: false,
    };

    let e2 = MergeIteratorElement {
        nice: 1,
        itr: TestIterator {
            key: b"abc".to_vec(),
        },
        reverse: false,
    };
    let e3 = MergeIteratorElement {
        nice: 2,
        itr: TestIterator {
            key: b"abc".to_vec(),
        },
        reverse: false,
    };
    let e4 = MergeIteratorElement {
        nice: 2,
        itr: TestIterator {
            key: b"abc".to_vec(),
        },
        reverse: false,
    };

    let mut e = vec![e4, e1, e2, e3];
    e.sort();
    println!("{:?}", e);
    let mut heap = BinaryHeap::new();
    for element in e {
        heap.push(element);
    }
    let top = heap.pop().unwrap();
    println!("{:?}", top);
}

#[test]
fn heap() {}
