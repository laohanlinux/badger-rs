use std::io::Write;
use byteorder::{LittleEndian, WriteBytesExt};
use protobuf::Message;
use crate::pb::backup::KVPair;

pub fn write_to<W>(entry: &KVPair, wt: &mut W) -> crate::Result<()> where W: Write {
    let buf = entry.write_to_bytes().unwrap();
    wt.write_u64::<LittleEndian>(buf.len() as u64)?;
    wt.write_all(&buf)?;
    Ok(())
}
