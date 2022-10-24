use crate::skl::BlockBytes;
use crate::skl::Chunk;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::cmp::Ordering;
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

pub struct MergeIterator {}

pub struct MergeIteratorElement<V, I: Iterator + KeyValue<V>> {
    nice: isize,
    itr: I,
    reverse: bool,
    _data: PhantomData<V>,
}

impl<V, I> PartialEq for MergeIteratorElement<V, I>
where
    I: Iterator + KeyValue<ValueStruct>,
{
    fn eq(&self, other: &Self) -> bool {
        self.itr.key() == &other.itr.key()
    }
}

#[test]
fn heap() {
    #[derive(Default, Debug)]
    struct Person {
        pub id: u32,
        pub name: String,
        pub height: u64,
    }

    impl PartialEq for Person {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }
    impl Eq for Person {}

    impl PartialOrd for Person {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.height.partial_cmp(&self.height)
        }
    }

    // impl Ord for Person {
    //     fn cmp(&self, other: &Self) -> Ordering {
    //         self.height.cmp(&other.height)
    //     }
    // }

    let p1 = Person {
        id: 1,
        height: 1,
        ..Default::default()
    };
    let p2 = Person {
        id: 1,
        height: 2,
        ..Default::default()
    };

    let p3 = Person {
        id: 2,
        height: 3,
        ..Default::default()
    };

    assert_eq!(p1, p2);
    assert_ne!(p1, p3);
    assert_ne!(p2, p3);

    println!("{}", p1 < p2);
    assert!(p1 < p2);
    assert!(p2 < p3);
    assert!(p1 < p3);
}
