use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE};
use byteorder::{BigEndian, LittleEndian};
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde_json::{to_value, Value};
use std::io::Cursor;

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value|
#[derive(Debug, Clone, Default)]
pub struct ValueStruct<'a> {
    meta: u8,
    user_meta: u8,
    cas_counter: u64,
    pub(crate) value: &'a [u8],
}

impl<'a> ValueStruct<'a> {
    const VALUE_META_OFFSET: usize = 0;
    const VALUE_USER_META_OFFSET: usize = Self::VALUE_META_OFFSET + META_SIZE;
    const VALUE_CAS_OFFSET: usize = Self::VALUE_USER_META_OFFSET + USER_META_SIZE;
    const VALUE_VALUE_OFFSET: usize = Self::VALUE_CAS_OFFSET + CAS_SIZE;

    pub fn new(value: &'a [u8], meta: u8, user_meta: u8, cas_counter: u64) -> Self {
        Self {
            value,
            meta,
            user_meta,
            cas_counter,
        }
    }

    /// `encode_size` is the size of the ValueStruct when encoded
    pub fn encode_size(&self) -> usize {
        self.value.len() + Self::VALUE_VALUE_OFFSET
    }

    pub fn encode(&mut self, buffer: &mut [u8]) {
        buffer[Self::VALUE_META_OFFSET] = self.meta;
        buffer[Self::VALUE_USER_META_OFFSET] = self.user_meta;
        {
            let mut cursor =
                Cursor::new(&mut buffer[Self::VALUE_CAS_OFFSET..Self::VALUE_CAS_OFFSET + CAS_SIZE]);
            cursor.write_u64::<BigEndian>(self.cas_counter).unwrap();
        }
        buffer[Self::VALUE_VALUE_OFFSET..(Self::VALUE_VALUE_OFFSET + self.value.len())]
            .copy_from_slice(self.value);
    }
    // value_struct_serialized_size converts a value size to the full serialized size of value + metadata.
    pub fn value_struct_serialized_size(size: u16) -> usize {
        size as usize + Self::VALUE_VALUE_OFFSET
    }
}

/// decode_entries_slice uses the length of the slice to infer the length of the Value field.
impl<'a> From<&'a [u8]> for ValueStruct<'a> {
    fn from(mut buffer: &'a [u8]) -> Self {
        let mut v = ValueStruct::default();
        v.meta = *buffer.get(Self::VALUE_VALUE_OFFSET).unwrap();
        v.user_meta = *buffer.get(Self::VALUE_META_OFFSET).unwrap();
        // FIXME:
        let mut count_v = Vec::from_iter(
            buffer[Self::VALUE_CAS_OFFSET..(Self::VALUE_CAS_OFFSET + CAS_SIZE)].iter(),
        )
        .iter()
        .map(|u| **u)
        .collect::<Vec<_>>();
        v.cas_counter = Cursor::new(&mut count_v).read_u64::<BigEndian>().unwrap();
        v.value = &buffer[Self::VALUE_VALUE_OFFSET..];
        v
    }
}

impl<'a> From<ValueStruct<'a>> for Vec<u8> {
    fn from(v: ValueStruct) -> Self {
        let mut buffer = vec![0u8; v.encode_size()];
        buffer[ValueStruct::VALUE_META_OFFSET] = v.meta;
        buffer[ValueStruct::VALUE_USER_META_OFFSET] = v.user_meta;
        Cursor::new(
            &mut buffer[ValueStruct::VALUE_CAS_OFFSET..(ValueStruct::VALUE_CAS_OFFSET + CAS_SIZE)],
        )
        .write_u64::<BigEndian>(v.cas_counter)
        .unwrap();
        buffer
    }
}

#[test]
fn test_value_struct() {
    assert_eq!(ValueStruct::VALUE_VALUE_OFFSET, 10);
}
