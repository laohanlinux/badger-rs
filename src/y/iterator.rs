use crate::skl::BlockBytes;
use crate::skl::Chunk;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::cmp::Ordering;
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
    fn get_cur_kv(&self) -> Option<(&[u8], Self::Output)> {
        todo!()
    }
}

pub trait KeyValue<V> {
    fn key(&self) -> &[u8];
    fn value(&self) -> V;
}

#[derive(Debug, Eq)]
pub(crate) struct HeapItem<T: Eq> {
    idx: usize,
    element: T,
}

impl<T: Eq> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.idx.cmp(&self.idx)
    }
}

impl<T: Eq> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Eq> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx
    }
}

pub struct MergeIterator<T> {
    // h: BinaryHeap<HeapItem>,
    cur_key: Vec<u8>,
    reversed: bool,
    all: Vec<Box<dyn Iterator<Output = T>>>,
}

impl<T> MergeIterator<T>
where
    T: Iterator,
{
    fn store_key(&mut self, smallest: T) {
        let key = smallest.get_cur_kv().unwrap().0;
        self.cur_key.clear();
        self.cur_key.extend_from_slice(key);
    }
}

impl Iterator for MergeIterator<ValueStruct> {
    type Output = ValueStruct;

    fn next(&self) -> Option<Self::Output> {
        let smallest = self.all.first().unwrap();
        smallest.next()
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

#[test]
fn it() {
    // let ref v = ValueStruct::new(vec![1, 2, 3, 4, 5], 1, 2, 10);
    // let buffer: Vec<u8> = v.into();
    // let got: ValueStruct = buffer.as_slice().into();
    // assert_eq!(*v, got);
    // let mut h = BinaryHeap::new();
    // h.push(HeapItem {
    //     idx: 0,
    //     // element: 10,
    // });
    // h.push(HeapItem {
    //     idx: 0,
    //     // element: 100,
    // });
    // h.push(HeapItem {
    //     idx: 10,
    //     // element: 200,
    // });

    println!(
        " {}",
        HeapItem {
            idx: 110,
            element: 100
        } >= HeapItem {
            idx: 120,
            element: 200
        }
    );
}
