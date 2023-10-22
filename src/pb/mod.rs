// @generated

use protobuf::Message;
use crate::manifest::ManifestChangeBuilder;
use crate::pb::badgerpb3::ManifestChangeSet;
use crate::Result;
// use quick_protobuf::MessageWrite;

pub mod badgerpb3;
pub mod backup;

pub(crate) fn convert_manifest_set_to_vec(mf_set: &ManifestChangeSet) -> Vec<u8> {
    let mut buffer = vec![];
    mf_set.write_to_vec(&mut buffer).unwrap();
    buffer
}

pub(crate) fn parse_manifest_set_from_vec(buffer: &[u8]) -> Result<ManifestChangeSet> {
    let set: ManifestChangeSet = protobuf::Message::parse_from_bytes(buffer).map_err(|err| crate::Error::from(format!("{}", err)))?;
    Ok(set)
}

#[test]
fn enc_dec() {
    let mut mf = ManifestChangeSet::default();
    mf.changes
        .extend(vec![ManifestChangeBuilder::new(1).build()]);
    let buffer = convert_manifest_set_to_vec(&mf);
    let got = parse_manifest_set_from_vec(&buffer).unwrap();
    assert_eq!(got, mf);
}
