use crate::skl::BlockBytes;
use crate::skl::Chunk;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
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
    const fn header_size() -> usize {
        10
    }

    fn size(&self) -> usize {
        Self::header_size() + self.value.len()
    }

    pub(crate) fn get_data(&self) -> &[u8] {
        unsafe {
            let ptr = self as *const ValueStruct as *const u8;
            &*slice_from_raw_parts(ptr, self.size())
        }
    }

    fn get_data_mut(&self) -> &mut [u8] {
        unsafe {
            let ptr = self as *const ValueStruct as *mut u8;
            &mut *slice_from_raw_parts_mut(ptr, self.size())
        }
    }
}

impl From<&[u8]> for &ValueStruct {
    fn from(buffer: &[u8]) -> Self {
        unsafe {
            &*(buffer.as_ptr() as *mut ValueStruct)
        }
    }
}

#[test]
fn it() {
    let ref v = ValueStruct::new(vec![1, 2, 3, 4, 5], 1, 2, 10);
    let buffer = v.get_data();
    let got: &ValueStruct = buffer.into();
    assert_eq!(v, got);
}
