use crate::skl::BlockBytes;
use crate::skl::Chunk;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};
use crate::must_align;

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value_size|value|
#[derive(Debug, Default, PartialEq)]
#[repr(C)]
pub struct ValueStruct {
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) cas_counter: u64,
    pub(crate) value_sz: u32,
    // PhantomData not occupy real memory
    pub(crate) value: PhantomData<u8>,
}

impl ValueStruct {
    const fn header_size() -> usize {
        size_of::<Self>()
    }

    fn size(&self) -> usize {
        Self::header_size() + self.value_sz as usize
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        let ptr = self as *const Self as *const u8;
        unsafe {from_raw_parts(ptr, self.size())}
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self as *mut Self as *mut u8;
        unsafe {from_raw_parts_mut(ptr, self.size())}
    }

    #[inline]
    pub(crate) fn from_slice(buffer: &[u8]) -> &Self {
        unsafe { &*(buffer.as_ptr() as *const ValueStruct) }
    }

    #[inline]
    fn from_mut_slice(mut buffer: &mut [u8]) -> &mut Self {
        unsafe { &mut *(buffer.as_mut_ptr() as *mut ValueStruct) }
    }

    #[inline]
    pub(crate) fn value(&self) -> &[u8] {
        must_align(self);
        unsafe {
            let optr = self.as_ptr();
            let ptr = optr.add(self.size());
            from_raw_parts(ptr, self.value_sz as usize)
        }
    }

    #[inline]
    const fn as_ptr(&self) -> *const u8 {
        self as *const Self as *const u8
    }
}
//
// impl<T> From<T> for ValueStruct where T: AsRef<[u8]> {
//     fn from(buffer: T) -> Self {
//         let mut v = ValueStruct::default();
//         v.read_data(buffer.as_ref());
//         v
//     }
// }
//
// impl Into<Vec<u8>> for &ValueStruct {
//     fn into(self) -> Vec<u8> {
//         let mut buffer = vec![0; self.size()];
//         self.write_data(&mut buffer);
//         println!("{:?}", buffer);
//         buffer
//     }
// }

#[test]
fn it() {
    // let ref v = ValueStruct::new(vec![1, 2, 3, 4, 5], 1, 2, 10);
    // let buffer: Vec<u8> = v.into();
    // let got: ValueStruct = buffer.as_slice().into();
    // assert_eq!(*v, got);
}
