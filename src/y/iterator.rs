use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::{BigEndian, LittleEndian};
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde_json::{to_value, Value};
use std::io::Cursor;
use std::marker::PhantomData;
use std::mem::size_of;
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value_size|value|
#[derive(Debug, Clone, Default)]
pub struct ValueStruct {
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) cas_counter: u64,
    pub(crate) value_size: u16,
    pub(crate) value: PhantomData<u8>,
}

impl ValueStruct {
    fn header_size() -> usize {
        size_of::<ValueStruct>()
    }
    #[inline]
    pub fn get_value(&self) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { from_raw_parts(ptr, self.value_size as usize) }
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        &self.value as *const PhantomData<u8> as *const u8
    }

    #[inline]
    pub(crate) fn get_data_mut_ptr(&mut self) -> *mut u8 {
        &mut self.value as *mut PhantomData<u8> as *mut u8
    }

    #[inline]
    pub(crate) fn from_slice(buffer: &[u8]) -> &Self {
        unsafe { &*(buffer.as_ptr() as *const ValueStruct) }
    }

    #[inline]
    pub(crate) fn from_slice_mut(mut buffer: &mut [u8]) -> &mut Self {
        unsafe { &mut *(buffer.as_mut_ptr() as *mut ValueStruct) }
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        let ptr = self as *const Self as *const u8;
        unsafe { from_raw_parts(ptr, self.byte_size()) }
    }

    #[inline]
    pub(crate) fn as_slice_mut(&mut self) -> &mut [u8] {
        let ptr = self as *mut Self as *mut u8;
        unsafe { from_raw_parts_mut(ptr, self.byte_size()) }
    }

    pub(crate) fn byte_size(&self) -> usize {
        Self::header_size() + self.value_size as usize
    }
}
