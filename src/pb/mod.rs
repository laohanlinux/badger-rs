// @generated

use crate::pb::badgerpb3::ManifestChangeSet;
use crate::Result;
use quick_protobuf::MessageWrite;

pub mod badgerpb3;

pub(crate) fn convert_manifest_set_to_vec(mf_set: &ManifestChangeSet) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(mf_set.get_size());
    let mut cursor = quick_protobuf::writer::Writer::new(&mut buffer);
    buffer
}

pub(crate) fn parse_manifest_set_from_vec(buffer: &[u8]) -> Result<ManifestChangeSet> {
    let set: ManifestChangeSet = quick_protobuf::reader::deserialize_from_slice(buffer)
        .map_err(|err| crate::Error::from(format!("{}", err)))?;
    Ok(set)
}
