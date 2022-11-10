use parking_lot::RwLock;
use std::fs::File;
use crate::Result;

/// TableManifest contains information about a specific level
/// in the LSM tree.
#[derive(Default)]
pub struct TableManifest {
    Level: u8,
}

#[derive(Default)]
pub(crate) struct ManifestFile {
    fp: Option<File>,
    directory: String,
    // We make this configurable so that unit tests can hit rewrite() code quickly
    deletions_rewrite_threshold: usize,
    // Guards appends, which includes access to the manifest field.
    append_lock: RwLock<()>,

    // Used to track the current state of the manifest, used when rewriting.
    manifest: Manifest,
}
/// Manifest represnts the contents of the MANIFEST file in a Badger store.
///
/// The MANIFEST file describes the startup state of the db -- all LSM files and what level they're
/// at.
///
/// It consists of a sequence of ManifestChangeSet objects.  Each of these is treated atomically,
/// and contains a sequence of ManifestChange's (file creations/deletions) which we use to
/// reconstruct the manifest at startup.
#[derive(Default)]
pub struct Manifest {}

// todo
pub(crate) fn open_or_create_manifest_file(dir: &str) -> Result<(ManifestFile, Manifest)>{
    Ok((ManifestFile::default(), Manifest::default()))
}
