// use crate::pb::badgerpb3::{ManifestChange, ManifestChangeSet, ManifestChange_Operation};
use crate::pb::badgerpb3::manifest_change::Operation;
use crate::pb::badgerpb3::{ManifestChange, ManifestChangeSet};
use crate::y::is_eof;
use crate::Error::{BadMagic, Unexpected};
use crate::{Error, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::buf::Reader;
use libc::bind;
use parking_lot::RwLock;
use protobuf::{Enum, EnumOrUnknown, Message};
use std::collections::{HashMap, HashSet};
use std::fs::{rename, File};
use std::io::{Cursor, Read, Write};
use std::path::Path;

// Manifest file
const MANIFEST_FILENAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILENAME: &str = "MANIFEST-REWRITE";
const MANIFEST_DELETIONS_REWRITE_THRESHOLD: usize = 10000;
const MANIFEST_DELETIONS_RATIO: usize = 10;

// Has to be 4 bytes. The value can never change, ever, anyway.
const MAGIC_TEXT: &[u8; 4] = b"bdgr";

// The magic version number
const MAGIC_VERSION: u32 = 2;

/// Contains information about LSM tree levels
/// in the *MANIFEST* file.
#[derive(Default)]
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

    /// Reads the manifest file and constructs two manifest objects. (We need one immutable
    /// copy and one mutable copy of the manifest. Easiest way is to construct two of them.)
    /// Also, returns the last offset after a completely read manifest entry -- the file must be
    /// truncated at that point before further appends are made (if there is a partial entry after
    /// that). In normal conditions, trunc_offset is the file size.
    pub fn replay_manifest_file(fp: &mut File) -> Result<(Manifest, usize)> {
        let mut magic = vec![0u8; 4];
        if fp.read(&mut magic)? != 4 {
            return Err(BadMagic);
        }
        if MAGIC_TEXT[..] != magic[..4] {
            return Err(BadMagic);
        }
        if MAGIC_VERSION != fp.read_u32::<BigEndian>()? {
            return Err(BadMagic);
        }

        let mut build = Manifest::new();
        let mut offset = 8;
        loop {
            let sz = fp.read_u32::<BigEndian>()?;
            let crc32 = fp.read_u32::<BigEndian>()?;
            offset += 8;
            let mut buffer = vec![0u8; sz as usize];
            offset += fp.read(&mut buffer)?;
            if crc32 != crc32fast::hash(&buffer) {
                // TODO why
                break;
            }
            let mut mf_set = ManifestChangeSet::parse_from_bytes(&buffer).map_err(|_| BadMagic)?;
            apply_manifest_change_set(&mut build, &mf_set)?;
        }

        Ok((build, offset))
    }

    pub fn rewrite(&self, dir: &str) -> Result<(File, usize)> {
        let rewrite_path = Path::new(dir).join(MANIFEST_REWRITE_FILENAME);
        // We explicitly sync.
        let mut fp = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&rewrite_path)?;
        let mut wt = Cursor::new(vec![]);
        wt.write_all(MAGIC_TEXT)?;
        wt.write_u32::<BigEndian>(MAGIC_VERSION)?;

        let net_creations = self.tables.len();
        let mut mf_set = ManifestChangeSet::new();
        mf_set.changes = self.as_changes();
        let mf_buffer = mf_set.write_to_bytes().unwrap();
        wt.write_u32::<BigEndian>(mf_buffer.len() as u32)?;
        let crc32 = crc32fast::hash(&*mf_buffer);
        wt.write_u32::<BigEndian>(crc32)?;
        wt.write_all(&*mf_buffer)?;
        fp.write_all(&*wt.into_inner())?;
        fp.sync_all()?;
        drop(fp);

        let manifest_path = Path::new(dir).join(MANIFEST_FILENAME);
        rename(&rewrite_path, &manifest_path)?;
        // TODO add directory sync

        let fp = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(manifest_path)?;
        Ok((fp, net_creations))
    }

    fn as_changes(&self) -> Vec<ManifestChange> {
        self.tables
            .iter()
            .map(|(id, tb)| {
                ManifestChangeBuilder::new(*id)
                    .with_op(Operation::CREATE)
                    .with_level(tb.level as u32)
                    .build()
            })
            .collect::<Vec<_>>()
    }
}

// this is not a "recoverable" error -- opening the KV store fails because the MANIFEST file
// is just plain broken.
fn apply_manifest_change_set(build: &mut Manifest, mf_set: &ManifestChangeSet) -> Result<()> {
    for change in mf_set.changes.iter() {
        apply_manifest_change(build, change)?;
    }
    Ok(())
}

fn apply_manifest_change(build: &mut Manifest, tc: &ManifestChange) -> Result<()> {
    let op = Operation::from_i32(tc.Op.value()).unwrap();
    match op {
        Operation::CREATE => {
            if build.tables.contains_key(&tc.Id) {
                return Err(Unexpected(format!(
                    "MANIFEST invalid, table {} exists",
                    tc.Id
                )));
            }
            let table_mf = TableManifest {
                level: tc.Level as u8,
            };
            for _ in build.levels.len()..=tc.Level as usize {
                build.levels.push(LevelManifest::default());
            }
            build.tables.insert(tc.Id, table_mf);
            build.levels[tc.Level as usize].tables.insert(tc.Id);
            build.creations += 1;
        }

        Operation::DELETE => {
            let has = build.tables.remove(&tc.Id);
            if has.is_none() {
                return Err(Unexpected(format!(
                    "MANIFEST removes non-existing table {}",
                    tc.Id
                )));
            }
            let has = build
                .levels
                .get_mut(tc.Level as usize)
                .unwrap()
                .tables
                .remove(&tc.Id);
            assert!(has);
        }
        _ => {
            return Err(Unexpected(
                "MANIFEST file has invalid manifest_change op".into(),
            ))
        }
    }

    Ok(())
}

pub(crate) fn open_or_create_manifest_file(dir: &str) -> Result<(ManifestFile, Manifest)> {
    Ok((ManifestFile::default(), Manifest::default()))
}

#[derive(Debug)]
struct ManifestChangeBuilder {
    id: u64,
    level: u32,
    op: Operation,
}

impl ManifestChangeBuilder {
    fn new(id: u64) -> Self {
        ManifestChangeBuilder {
            id,
            level: 0,
            op: Operation::CREATE,
        }
    }

    // fn id(mut self, id: u64) -> Self {
    //     self.id = id;
    //     self
    // }

    fn with_level(mut self, level: u32) -> Self {
        self.level = level;
        self
    }

    fn with_op(mut self, op: Operation) -> Self {
        self.op = op;
        self
    }

    fn build(self) -> ManifestChange {
        let mut mf = ManifestChange::new();
        mf.Id = self.id;
        mf.Level = self.level;
        mf.Op = EnumOrUnknown::new(self.op);
        mf
    }
}
