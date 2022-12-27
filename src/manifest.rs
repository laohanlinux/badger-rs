use std::collections::{HashMap, HashSet};
use crate::Result;
use parking_lot::RwLock;
use std::fs::File;

// Manifest file
const MANIFEST_FILENAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILENAME: &str = "MANIFEST-REWRITE";
const MANIFEST_DELETIONS_REWRITE_THRESHOLD: usize = 10000;
const MANIFEST_DELETIONS_RATIO: usize = 10;


/// Contains information about LSM tree levels
/// in the *MANIFEST* file.
pub struct LevelManifest {
    tables: HashSet<u64>, // Set of table id's
}

/// *TableManifest* contains information about a specific level
/// in the LSM tree.
#[derive(Default)]
pub struct TableManifest {
    level: u8,
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

/// Manifest represents the contents of the MANIFEST file in a Badger store.
///
/// The MANIFEST file describes the startup state of the db -- all LSM files and what level they're
/// at.
///
/// It consists of a sequence of ManifestChangeSet objects.  Each of these is treated atomically,
/// and contains a sequence of ManifestChange's (file creations/deletions) which we use to
/// reconstruct the manifest at startup.
#[derive(Default)]
pub struct Manifest {
    levels: Vec<LevelManifest>,
    tables: HashMap<u64, TableManifest>,
    // Contains total number of creation and deletion changes in the manifest --- used to compute
    // whether it'd be useful to rewrite the manifest
    creations: usize,
    deletions: usize,
}

impl Manifest {
    pub fn new() -> Self {
        Manifest {
            levels: vec![],
            tables: HashMap::default(),
            creations: Default::default(),
            deletions: Default::default(),
        }
    }
}

// todo
pub(crate) fn open_or_create_manifest_file(dir: &str) -> Result<(ManifestFile, Manifest)> {
    Ok((ManifestFile::default(), Manifest::default()))
}
