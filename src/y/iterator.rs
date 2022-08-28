use crate::skl::Slice;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::marker::PhantomData;
use std::mem::size_of;
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value_size|value|
#[derive(Debug)]
#[repr(C)]
pub struct ValueStruct {
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) cas_counter: u64,
    pub(crate) value: Slice,
}

impl Default for ValueStruct {
    fn default() -> Self {
        ValueStruct {
            meta: 0,
            user_meta: 0,
            cas_counter: 0,
            value: Slice::new(0 as *const u8, 0),
        }
    }
}

impl ValueStruct {
    const fn header_size() -> usize {
        10
    }
    fn size(&self) -> usize {
        Self::header_size() + self.value.size()
    }
    pub(crate) fn to_vec(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; self.size()];
        buffer[0] = self.meta;
        buffer[1] = self.user_meta;
        {
            let mut cursor = Cursor::new(&mut buffer[2..ValueStruct::header_size()]);
            cursor.write_u64::<BigEndian>(self.cas_counter);
        }
        buffer[ValueStruct::header_size()..].copy_from_slice(self.value.get_data());
        buffer
    }
}

impl From<Slice> for ValueStruct {
    fn from(slice: Slice) -> Self {
        let data = slice.get_data();
        let total_sz = data.len();
        let mut value = ValueStruct::default();
        value.meta = data[0];
        value.user_meta = data[1];
        value.cas_counter = Cursor::new(&data[2..ValueStruct::header_size()])
            .read_u64::<BigEndian>()
            .unwrap();
        {
            let ptr = data[ValueStruct::header_size()..].as_ptr();
            value.value = Slice::new(ptr, total_sz - ValueStruct::header_size());
        }
        value
    }
}

impl Into<Vec<u8>> for ValueStruct {
    fn into(self) -> Vec<u8> {
        let mut buffer = vec![0u8; self.size()];
        buffer[0] = self.meta;
        buffer[1] = self.user_meta;
        {
            let mut cursor = Cursor::new(&mut buffer[2..ValueStruct::header_size()]);
            cursor.write_u64::<BigEndian>(self.cas_counter);
        }
        buffer[ValueStruct::header_size()..].copy_from_slice(self.value.get_data());
        buffer
    }
}

#[test]
fn it() {}
