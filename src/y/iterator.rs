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
    pub(crate) value: Vec<u8>,
}

impl ValueStruct {
    pub(crate) fn new(value: Vec<u8>, meta: u8, user_meta: u8, cas_counter: u64) -> ValueStruct {
        ValueStruct {
            meta,
            user_meta,
            cas_counter,
            value_sz: value.len() as u32,
            value,
        }
    }
    const fn header_size() -> usize {
        14
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
        cursor.write_u32::<BigEndian>(self.value_sz).unwrap();
        cursor.write_all(&self.value).unwrap();
    }

    pub(crate) fn read_data(&mut self, buffer: &[u8]) {
        use std::io::Read;
        println!("{:?}", buffer);
        let mut cursor = Cursor::new(buffer);
        self.meta = cursor.read_u8().unwrap();
        self.user_meta = cursor.read_u8().unwrap();
        self.cas_counter = cursor.read_u64::<BigEndian>().unwrap();
        self.value_sz = cursor.read_u32::<BigEndian>().unwrap();
        self.value = vec![0; self.value_sz as usize];
        cursor.read_exact(&mut self.value).unwrap();
    }

    // pub(crate) fn get_data(&self) -> &[u8] {
    //     unsafe {
    //         let ptr = self as *const ValueStruct as *const u8;
    //         &*slice_from_raw_parts(ptr, self.size())
    //     }
    // }
    //
    // fn get_data_mut(&self) -> &mut [u8] {
    //     unsafe {
    //         let ptr = self as *const ValueStruct as *mut u8;
    //         &mut *slice_from_raw_parts_mut(ptr, self.size())
    //     }
    // }
}

impl <T> From<T> for ValueStruct where T: AsRef<[u8]> {
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
        println!("{:?}", buffer);
        buffer
    }
}

#[test]
fn it() {
    let ref v = ValueStruct::new(vec![1, 2, 3, 4, 5], 1, 2, 10);
    let buffer: Vec<u8> = v.into();
    let got: ValueStruct = buffer.as_slice().into();
    assert_eq!(*v, got);
}
