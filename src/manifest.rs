// use crate::pb::badgerpb3::{ManifestChange, ManifestChangeSet, ManifestChange_Operation};
use crate::pb::badgerpb3::manifest_change::Operation;
use crate::pb::badgerpb3::{ManifestChange, ManifestChangeSet};
use crate::y::{is_eof, open_existing_synced_file};
use crate::Error::{BadMagic, Unexpected};
use crate::{is_existing, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::info;
use parking_lot::RwLock;
use protobuf::{Enum, EnumOrUnknown, Message};
use std::collections::{HashMap, HashSet};
use std::fs::{rename, File};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

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
#[derive(Default, Clone)]
pub struct LevelManifest {
    tables: HashSet<u64>, // Set of table id's
}

/// *TableManifest* contains information about a specific level
/// in the LSM tree.
#[derive(Default, Clone)]
pub struct TableManifest {
    pub level: u8,
}

#[derive(Default)]
pub struct ManifestFile {
    fp: Option<File>,
    directory: String,
    // We make this configurable so that unit tests can hit rewrite() code quickly
    deletions_rewrite_threshold: AtomicU32,

    // Access must be with a lock.
    // Used to track the current state of the manifest, used when rewriting.
    manifest: Arc<tokio::sync::RwLock<Manifest>>,
}

impl ManifestFile {
    /// Write a batch of changes, atomically, to the file. By "atomically" that means when
    /// we replay the *MANIFEST* file, we'll either replay all the changes or none of them. (The truth of
    /// this depends on the filesystem)
    pub async fn add_changes(&mut self, changes: Vec<ManifestChange>) -> Result<()> {
        let mut mf_changes = ManifestChangeSet::new();
        mf_changes.changes.extend(changes);
        let mf_buffer = mf_changes.write_to_bytes().unwrap();
        // Maybe we could user O_APPEND instead (on certain file systems)
        apply_manifest_change_set(self.manifest.clone(), &mf_changes).await?;
        // Rewrite manifest if it'd shrink by 1/10, and it's big enough to care
        let rewrite = {
            let mf_lck = self.manifest.read().await;
            mf_lck.deletions
                > self
                    .deletions_rewrite_threshold
                    .load(atomic::Ordering::Relaxed) as usize
                && mf_lck.deletions
                    > MANIFEST_DELETIONS_RATIO * (mf_lck.creations - mf_lck.deletions)
        };
        if rewrite {
            self.rewrite().await?;
        } else {
            let mut buffer = Cursor::new(vec![]);
            buffer.write_u32::<BigEndian>(mf_buffer.len() as u32)?;
            let crc32 = crc32fast::hash(&mf_buffer);
            buffer.write_u32::<BigEndian>(crc32)?;
            buffer.write_all(&mf_buffer)?;
            self.fp.as_mut().unwrap().write_all(&buffer.into_inner())?;
        }
        self.fp.as_mut().unwrap().sync_all()?;
        Ok(())
    }

    /// Must be called while appendLock is held.
    pub async fn rewrite(&mut self) -> Result<()> {
        {
            self.fp.take();
        }
        let (fp, n) = Self::help_rewrite(&self.directory, &self.manifest).await?;
        self.fp = Some(fp);
        let mut m_lck = self.manifest.write().await;
        m_lck.creations = n;
        m_lck.deletions = 0;
        Ok(())
    }

    async fn help_rewrite(
        dir: &str,
        m: &Arc<tokio::sync::RwLock<Manifest>>,
    ) -> Result<(File, usize)> {
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

        let m_lck = m.read().await;
        let net_creations = m_lck.tables.len();
        let mut mf_set = ManifestChangeSet::new();
        mf_set.changes = m_lck.as_changes();
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

    async fn open_or_create_manifest_file(
        dir: &str,
        deletions_threshold: u32,
    ) -> Result<ManifestFile> {
        let path = Path::new(dir).join(MANIFEST_FILENAME);
        // We explicitly sync in add_changes, outside the lock.
        let fp = open_existing_synced_file(path.to_str().unwrap(), false);
        return match fp {
            Ok(mut fp) => {
                let (manifest, trunc_offset) = Manifest::replay_manifest_file(&mut fp).await?;
                fp.set_len(trunc_offset as u64)?;
                fp.seek(SeekFrom::End(0))?;
                info!("recover a new manifest, offset: {}", trunc_offset);
                Ok(ManifestFile {
                    fp: Some(fp),
                    directory: dir.to_string(),
                    deletions_rewrite_threshold: AtomicU32::new(deletions_threshold),
                    manifest: Arc::new(tokio::sync::RwLock::new(manifest)),
                })
            }
            Err(err) if err.is_io_notfound() => {
                let mf = Arc::new(tokio::sync::RwLock::new(Manifest::new()));
                let (fp, n) = Self::help_rewrite(dir, &mf).await?;
                assert_eq!(n, 0);
                info!("create a new manifest");
                Ok(ManifestFile {
                    fp: Some(fp),
                    directory: dir.to_string(),
                    deletions_rewrite_threshold: AtomicU32::new(deletions_threshold),
                    manifest: mf,
                })
            }
            Err(err) => Err(err),
        };
    }
}

/// Manifest represents the contents of the MANIFEST file in a Badger store.
///
/// The MANIFEST file describes the startup state of the db -- all LSM files and what level they're
/// at.
///
/// It consists of a sequence of ManifestChangeSet objects.  Each of these is treated atomically,
/// and contains a sequence of ManifestChange's (file creations/deletions) which we use to
/// reconstruct the manifest at startup.
#[derive(Default, Clone)]
pub struct Manifest {
    levels: Vec<LevelManifest>,
    pub(crate) tables: HashMap<u64, TableManifest>,
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
    pub async fn replay_manifest_file(fp: &mut File) -> Result<(Manifest, usize)> {
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

        let build = Arc::new(tokio::sync::RwLock::new(Manifest::new()));
        let mut offset = 8;
        loop {
            let sz = fp.read_u32::<BigEndian>();
            if is_eof(&sz) {
                break;
            }
            let sz = sz?;
            let crc32 = fp.read_u32::<BigEndian>();
            if is_eof(&crc32) {
                break;
            }
            let crc32 = crc32?;
            let mut buffer = vec![0u8; sz as usize];
            assert_eq!(sz as usize, fp.read(&mut buffer)?);
            if crc32 != crc32fast::hash(&buffer) {
                break;
            }
            let mf_set = ManifestChangeSet::parse_from_bytes(&buffer).map_err(|_| BadMagic)?;
            apply_manifest_change_set(build.clone(), &mf_set).await?;
            offset = offset + 8 + sz as usize;
        }

        let build = build.write().await.clone();
        // so, return the lasted ManifestFile
        Ok((build, offset))
    }

    fn help_rewrite(&self, dir: &str) -> Result<(File, usize)> {
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
async fn apply_manifest_change_set(
    build: Arc<tokio::sync::RwLock<Manifest>>,
    mf_set: &ManifestChangeSet,
) -> Result<()> {
    for change in mf_set.changes.iter() {
        apply_manifest_change(build.clone(), change).await?;
    }
    Ok(())
}

async fn apply_manifest_change(
    build: Arc<tokio::sync::RwLock<Manifest>>,
    tc: &ManifestChange,
) -> Result<()> {
    let op = Operation::from_i32(tc.Op.value()).unwrap();
    let mut build = build.write().await;
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
    }

    Ok(())
}

pub(crate) async fn open_or_create_manifest_file(dir: &str) -> Result<(ManifestFile, Manifest)> {
    let manifest = Arc::new(tokio::sync::RwLock::new(Manifest::new()));
    let (fp, sz) = ManifestFile::help_rewrite(dir, &manifest).await?;

    Ok((ManifestFile::default(), Manifest::default()))
}

pub(crate) async fn help_open_or_create_manifest_file(
    dir: &str,
) -> Result<(ManifestFile, Manifest)> {
    let fpath = Path::new(dir).join(MANIFEST_FILENAME);
    let fp = open_existing_synced_file(fpath.as_str().unwrap(), true);
    if fp.is_err() {
        if !is_existing(&fp.map_err()) {
            return Err(fp.unwrap_err());
        }
        let mt = Manifest::new();
        let fp = mt.help_rewrite(dir)?;
        // let fp = mt.help_rewrite(dir).await?;
    }
}

#[derive(Debug)]
pub(crate) struct ManifestChangeBuilder {
    id: u64,
    level: u32,
    op: Operation,
}

impl ManifestChangeBuilder {
    pub(crate) fn new(id: u64) -> Self {
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

    pub(crate) fn with_level(mut self, level: u32) -> Self {
        self.level = level;
        self
    }

    pub(crate) fn with_op(mut self, op: Operation) -> Self {
        self.op = op;
        self
    }

    pub(crate) fn build(self) -> ManifestChange {
        let mut mf = ManifestChange::new();
        mf.Id = self.id;
        mf.Level = self.level;
        mf.Op = EnumOrUnknown::new(self.op);
        mf
    }
}
