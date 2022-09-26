use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crate::skl::Chunk;
use std::io::Cursor;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::NonNull;
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};
use crate::skl::BlockBytes;

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value_size|value|
#[derive(Debug, Default, PartialEq)]
#[repr(C)]
pub struct ValueStruct {
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) cas_counter: u64,
    pub(crate) value:  Vec<u8>,
}

impl ValueStruct {
    const fn header_size() -> usize {
        10
    }

    fn size(&self) -> usize {
        Self::header_size() + self.value.len()
    }

    pub(crate) fn to_vec(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; self.size()];
        buffer[0] = self.meta;
        buffer[1] = self.user_meta;
        {
            let mut cursor = Cursor::new(&mut buffer[2..ValueStruct::header_size()]);
            cursor.write_u64::<BigEndian>(self.cas_counter);
        }
        buffer[ValueStruct::header_size()..].copy_from_slice(&self.value);
        buffer
    }
}

impl From<&[u8]> for ValueStruct {
    fn from(v: &[u8]) -> Self {
        let cas_counter  = Cursor::new(&v[2..10]).read_u64::<BigEndian>().unwrap();
        let value_sz = Cursor::new(&v[10..14]).read_u32::<BigEndian>().unwrap();
        ValueStruct{
           meta: v[0],
           user_meta: v[1],
           cas_counter,
           value: v[ValueStruct::header_size()..].to_vec(),
       }
    }
}

impl Into<Vec<u8>> for ValueStruct {
    fn into(self) -> Vec<u8> {
       self.to_vec()
    }
}

#[test]
fn it() {}
