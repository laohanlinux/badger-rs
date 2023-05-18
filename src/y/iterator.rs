use crate::skl::Chunk;
use crate::table::iterator::IteratorItem;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use log::info;
use std::borrow::{Borrow, Cow};
use std::cell::{Cell, RefCell, RefMut};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::io::{Cursor, Read, Write};
use std::iter::Iterator as stdIterator;
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};

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

/// A iterator trait
pub trait Xiterator {
    /// The iterator element
    type Output;
    /// Same to std iterator next
    fn next(&self) -> Option<Self::Output>;
    /// Seeks to first element (or last element for reverse iterator).
    fn rewind(&self) -> Option<Self::Output>;
    /// Seek with key, return a element that it's key >= key or <= key.
    fn seek(&self, key: &[u8]) -> Option<Self::Output>;
    /// Peek current element
    fn peek(&self) -> Option<Self::Output> {
        todo!()
    }
    /// The iterator indetify
    fn id(&self) -> String {
        return "unknown_id".to_owned();
    }

    /// Close the iterator
    fn close(&self) {
        info!("close the iterator: {}", self.id());
    }
}

pub trait KeyValue<V> {
    fn key(&self) -> &[u8];
    fn value(&self) -> V;
}
