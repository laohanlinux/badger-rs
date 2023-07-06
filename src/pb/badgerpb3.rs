// Automatically generated rust module for 'badgerpb3.proto' file

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]


use quick_protobuf::{MessageInfo, MessageRead, MessageWrite, BytesReader, Writer, WriterBackend, Result};
use quick_protobuf::sizeofs::*;
use super::*;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ManifestChangeSet {
    pub changes: Vec<badgerpb3::ManifestChange>,
}

impl<'a> MessageRead<'a> for ManifestChangeSet {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.changes.push(r.read_message::<badgerpb3::ManifestChange>(bytes)?),
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for ManifestChangeSet {
    fn get_size(&self) -> usize {
        0
        + self.changes.iter().map(|s| 1 + sizeof_len((s).get_size())).sum::<usize>()
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.changes { w.write_with_tag(10, |w| w.write_message(s))?; }
        Ok(())
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Default, PartialEq, Clone)]
pub struct ManifestChange {
    pub id: u64,
    pub op: badgerpb3::mod_ManifestChange::Operation,
    pub level: u32,
}

impl<'a> MessageRead<'a> for ManifestChange {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(8) => msg.id = r.read_uint64(bytes)?,
                Ok(16) => msg.op = r.read_enum(bytes)?,
                Ok(24) => msg.level = r.read_uint32(bytes)?,
                Ok(t) => { r.read_unknown(bytes, t)?; }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}

impl MessageWrite for ManifestChange {
    fn get_size(&self) -> usize {
        0
        + if self.id == 0u64 { 0 } else { 1 + sizeof_varint(*(&self.id) as u64) }
        + if self.op == badgerpb3::mod_ManifestChange::Operation::CREATE { 0 } else { 1 + sizeof_varint(*(&self.op) as u64) }
        + if self.level == 0u32 { 0 } else { 1 + sizeof_varint(*(&self.level) as u64) }
    }

    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if self.id != 0u64 { w.write_with_tag(8, |w| w.write_uint64(*&self.id))?; }
        if self.op != badgerpb3::mod_ManifestChange::Operation::CREATE { w.write_with_tag(16, |w| w.write_enum(*&self.op as i32))?; }
        if self.level != 0u32 { w.write_with_tag(24, |w| w.write_uint32(*&self.level))?; }
        Ok(())
    }
}

pub mod mod_ManifestChange {


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Operation {
    CREATE = 0,
    DELETE = 1,
}

impl Default for Operation {
    fn default() -> Self {
        Operation::CREATE
    }
}

impl From<i32> for Operation {
    fn from(i: i32) -> Self {
        match i {
            0 => Operation::CREATE,
            1 => Operation::DELETE,
            _ => Self::default(),
        }
    }
}

impl<'a> From<&'a str> for Operation {
    fn from(s: &'a str) -> Self {
        match s {
            "CREATE" => Operation::CREATE,
            "DELETE" => Operation::DELETE,
            _ => Self::default(),
        }
    }
}

}

