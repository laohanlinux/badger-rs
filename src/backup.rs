use crate::pb::backup::KVPair;
use byteorder::{LittleEndian, WriteBytesExt};
use protobuf::Message;
use std::io::Write;

pub fn write_to<W>(entry: &KVPair, wt: &mut W) -> crate::Result<()>
where
    W: Write,
{
    let buf = entry.write_to_bytes().unwrap();
    wt.write_u64::<LittleEndian>(buf.len() as u64)?;
    wt.write_all(&buf)?;
    Ok(())
}
