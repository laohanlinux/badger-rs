// use crate::pb::badgerpb3::{ManifestChange, ManifestChangeSet, ManifestChange_Operation};
use crate::pb::badgerpb3::mod_ManifestChange::Operation::{self, CREATE, DELETE};
use crate::pb::badgerpb3::{ManifestChange, ManifestChangeSet};
use crate::types::TArcRW;
use crate::y::{hex_str, is_eof, open_existing_synced_file, sync_directory};
use crate::Error::{BadMagic, Unexpected};
use crate::Result;
use drop_cell::defer;
use log::info;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

use protobuf::{Enum, EnumOrUnknown, Message};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::fs::{rename, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::SystemTime;

use crate::pb::{convert_manifest_set_to_vec, parse_manifest_set_from_vec};
use quick_protobuf::MessageRead;
use quick_protobuf::{BytesReader, MessageWrite};
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

// Manifest file
const MANIFEST_FILENAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILENAME: &str = "MANIFEST-REWRITE";
const MANIFEST_DELETIONS_REWRITE_THRESHOLD: u32 = 10000;
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
    pub(crate) fp: Option<tokio::fs::File>,
    pub(crate) directory: String,
    // We make this configurable so that unit tests can hit rewrite() code quickly
    pub(crate) deletions_rewrite_threshold: AtomicU32,

    // Access must be with a lock.
    // Used to track the current state of the manifest, used when rewriting.
    pub(crate) manifest: TArcRW<Manifest>,
}

impl ManifestFile {
    /// Write a batch of changes, atomically, to the file. By "atomically" that means when
    /// we replay the *MANIFEST* file, we'll either replay all the changes or none of them. (The truth of
    /// this depends on the filesystem)
    pub async fn add_changes(&mut self, changes: Vec<ManifestChange>) -> Result<()> {
        let start = SystemTime::now();
        defer! {
            let took = SystemTime::now().duration_since(start).unwrap();
            info!("cost time at manifest add changes, {}ms", took.as_millis());
        }
        let mut mf_changes = ManifestChangeSet::default();
        mf_changes.changes.extend(changes);
        // info!("{:?}", self.manifest.read().await.tables.keys());
        // Maybe we could user O_APPEND instead (on certain file systems)
        apply_manifest_change_set(self.manifest.clone(), &mf_changes).await?;
        // Rewrite manifest if it'd shrink by 1/10, and it's big enough to care
        let rewrite = {
            let mf_lck = self.manifest.read().await;
            info!("{}, {}", mf_lck.creations, mf_lck.deletions);
            mf_lck.deletions
                > self
                    .deletions_rewrite_threshold
                    .load(atomic::Ordering::Relaxed) as usize
                && mf_lck.deletions
                    > MANIFEST_DELETIONS_RATIO * (mf_lck.creations - mf_lck.deletions)
        };
        if rewrite {
            info!("need to rewrite manifest file");
            self.rewrite().await?;
        } else {
            let mf_set_content = convert_manifest_set_to_vec(&mf_changes);
            let mut buffer = Vec::with_capacity(mf_set_content.len() + 4 + 4);
            buffer.write_u32(mf_set_content.len() as u32).await?;
            let crc32 = crc32fast::hash(&mf_set_content);
            buffer.write_u32(crc32).await?;
            tokio::io::AsyncWriteExt::write_all(&mut buffer, &mf_set_content).await?;
            self.fp.as_mut().unwrap().write_all(&buffer).await?;
        }
        self.fp.as_mut().unwrap().flush().await?;
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
        info!("Finished rewrite manifest file, tables count: {}", n);
        Ok(())
    }

    // A helper for rewrite manifest file.
    async fn help_rewrite(dir: &str, m: &TArcRW<Manifest>) -> Result<(tokio::fs::File, usize)> {
        let rewrite_path = Path::new(dir).join(MANIFEST_REWRITE_FILENAME);
        // We explicitly sync.
        let fp = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&rewrite_path)?;
        let mut fp = tokio::fs::File::from_std(fp);
        let mut wt = tokio::io::BufWriter::new(vec![]);
        {
            wt.write_all(MAGIC_TEXT).await?;
            wt.write_u32(MAGIC_VERSION).await?;
        }

        let m_lck = m.read().await;
        let net_creations = m_lck.tables.len();

        {
            let mut mf_set = ManifestChangeSet::default();
            mf_set.changes = m_lck.as_changes();
            let mf_buffer = convert_manifest_set_to_vec(&mf_set);
            wt.write_u32(mf_buffer.len() as u32).await?;
            let crc32 = crc32fast::hash(&*mf_buffer);
            wt.write_u32(crc32).await?;
            wt.write_all(&*mf_buffer).await?;
        }

        // persistent
        fp.write_all(&*wt.into_inner()).await?;
        fp.sync_all().await?;
        drop(fp);

        let manifest_path = Path::new(dir).join(MANIFEST_FILENAME);
        rename(&rewrite_path, &manifest_path)?;
        // TODO add directory sync

        let fp = File::options().write(true).read(true).open(manifest_path)?;
        Ok((tokio::fs::File::from_std(fp), net_creations))
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
                let mut fp = tokio::fs::File::from_std(fp);
                let (manifest, trunc_offset) = Manifest::replay_manifest_file(&mut fp).await?;
                fp.set_len(trunc_offset as u64).await?;
                fp.seek(SeekFrom::End(0)).await?;
                info!("recover a new manifest, offset: {}", trunc_offset);
                Ok(ManifestFile {
                    fp: Some(fp),
                    directory: dir.to_string(),
                    deletions_rewrite_threshold: AtomicU32::new(deletions_threshold),
                    manifest: Arc::new(RwLock::new(manifest)),
                })
            }
            Err(err) if err.is_io_notfound() => {
                let mf = Arc::new(RwLock::new(Manifest::new()));
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

    pub(crate) fn close(&mut self) {
        self.fp.take();
    }

    pub(crate) async fn to_string(&self) -> String {
        let mf = self.manifest.read().await;
        let mut buffer = std::io::Cursor::new(vec![]);
        std::io::Write::write_all(
            &mut buffer,
            format!("create: {}, delete: {}", mf.creations, mf.deletions).as_bytes(),
        )
        .unwrap();
        let tids = mf
            .tables
            .iter()
            .map(|(tid, _)| tid.to_string())
            .collect::<Vec<_>>()
            .join(",");
        std::io::Write::write_all(&mut buffer, tids.as_bytes()).unwrap();
        hex_str(&buffer.into_inner())
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

impl Display for Manifest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use std::io::Write;
        let mut buffer = std::io::Cursor::new(vec![]);
        Write::write_all(&mut buffer, b"Manifest").unwrap();
        Write::write_all(&mut buffer, b"Levels\n").unwrap();
        for (level, mf) in self.levels.iter().enumerate() {
            let tables = mf
                .tables
                .iter()
                .map(|tb| tb.to_string())
                .collect::<Vec<_>>();
            Write::write_all(&mut buffer, format!("{}=>", level).as_bytes()).unwrap();
            Write::write_all(&mut buffer, tables.join(",").as_bytes()).unwrap();
        }
        Write::write_all(&mut buffer, b"\n").unwrap();
        for (tb, mf) in self.tables.iter() {
            Write::write_all(&mut buffer, format!("{}=>{}", tb, mf.level).as_bytes()).unwrap();
        }
        let str = buffer.into_inner();
        write!(f, "{}", hex_str(&str))
    }
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
    pub async fn replay_manifest_file(fp: &mut tokio::fs::File) -> Result<(Manifest, usize)> {
        let mut magic = vec![0u8; 4];
        if fp.read(&mut magic).await? != 4 {
            return Err(BadMagic);
        }
        if MAGIC_TEXT[..] != magic[..4] {
            return Err(BadMagic);
        }
        if MAGIC_VERSION != fp.read_u32().await? {
            return Err(BadMagic);
        }

        let build = Arc::new(RwLock::new(Manifest::new()));
        let mut offset = 8;
        loop {
            let sz = fp.read_u32().await;
            if is_eof(&sz) {
                break;
            }
            let sz = sz?;
            let crc32 = fp.read_u32().await;
            if is_eof(&crc32) {
                break;
            }
            let crc32 = crc32?;
            let mut buffer = vec![0u8; sz as usize];
            assert_eq!(sz as usize, fp.read(&mut buffer).await?);
            if crc32 != crc32fast::hash(&buffer) {
                break;
            }

            let mf_set = parse_manifest_set_from_vec(&buffer).map_err(|_| BadMagic)?;
            apply_manifest_change_set(build.clone(), &mf_set).await?;
            offset = offset + 8 + sz as usize;
        }

        let build = build.write().await.clone();
        // so, return the lasted ManifestFile
        Ok((build, offset))
    }

    async fn help_rewrite(&self, dir: &str) -> Result<(tokio::fs::File, usize)> {
        use tokio::io::AsyncWriteExt;
        let rewrite_path = Path::new(dir).join(MANIFEST_REWRITE_FILENAME);
        // We explicitly sync.
        let fp = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&rewrite_path)?;
        let mut fp = tokio::fs::File::from_std(fp);
        let mut wt = tokio::io::BufWriter::new(vec![]);
        // write meta flags
        {
            wt.write_all(MAGIC_TEXT).await?;
            wt.write_u32(MAGIC_VERSION).await?;
        }
        let net_creations = self.tables.len();
        let mut mf_set = ManifestChangeSet::default();
        mf_set.changes = self.as_changes();
        let mf_buffer = convert_manifest_set_to_vec(&mf_set);

        wt.write_u32(mf_buffer.len() as u32).await?;
        let crc32 = crc32fast::hash(&*mf_buffer);
        wt.write_u32(crc32).await?;
        wt.write_all(&*mf_buffer).await?;
        fp.write_all(&*wt.into_inner()).await?;
        fp.flush().await?;
        fp.sync_all().await?;
        drop(fp);

        let manifest_path = Path::new(dir).join(MANIFEST_FILENAME);
        tokio::fs::rename(&rewrite_path, &manifest_path).await?;
        sync_directory(dir)?;
        let fp = open_existing_synced_file(manifest_path.as_os_str().to_str().unwrap(), false)?;
        let mut fp = tokio::fs::File::from_std(fp);
        fp.seek(SeekFrom::End(0)).await?;
        Ok((fp, net_creations))
    }

    fn as_changes(&self) -> Vec<ManifestChange> {
        self.tables
            .iter()
            .map(|(id, tb)| {
                ManifestChangeBuilder::new(*id)
                    .with_op(CREATE)
                    .with_level(tb.level as u32)
                    .build()
            })
            .collect::<Vec<_>>()
    }
}

// this is not a "recoverable" error -- opening the KV store fails because the MANIFEST file
// is just plain broken.
async fn apply_manifest_change_set(
    build: TArcRW<Manifest>,
    mf_set: &ManifestChangeSet,
) -> Result<()> {
    for change in mf_set.changes.iter() {
        apply_manifest_change(build.clone(), change).await?;
    }
    Ok(())
}

async fn apply_manifest_change(build: TArcRW<Manifest>, tc: &ManifestChange) -> Result<()> {
    let mut build = build.write().await;
    match tc.op {
        CREATE => {
            if build.tables.contains_key(&tc.id) {
                return Err(Unexpected(format!(
                    "MANIFEST invalid, table {} exists",
                    tc.id
                )));
            }
            let tid_level = TableManifest {
                level: tc.level as u8,
            };
            // expend levels numbers
            for _ in build.levels.len()..=tc.level as usize {
                build.levels.push(LevelManifest::default());
            }
            build.tables.insert(tc.id, tid_level);

            build.levels[tc.level as usize].tables.insert(tc.id);
            build.creations += 1;
        }

        DELETE => {
            // If the level is zero merge to level1 at first, it should be not include it ...
            let has = build.tables.remove(&tc.id);
            if has.is_none() {
                return Err(Unexpected(format!(
                    "MANIFEST removes non-existing table {}",
                    tc.id
                )));
            }

            let has = build
                .levels
                .get_mut(has.as_ref().unwrap().level as usize)
                .unwrap()
                .tables
                .remove(&tc.id);
            assert!(has);
            build.deletions += 1;
        }
    }

    Ok(())
}

pub(crate) async fn open_or_create_manifest_file(dir: &str) -> Result<ManifestFile> {
    help_open_or_create_manifest_file(dir, MANIFEST_DELETIONS_REWRITE_THRESHOLD).await
}

// Open it if not exist, otherwise create a new manifest file with dir directory
pub(crate) async fn help_open_or_create_manifest_file(
    dir: &str,
    deletions_threshold: u32,
) -> Result<ManifestFile> {
    let fpath = Path::new(dir).join(MANIFEST_FILENAME);
    let fpath = fpath.to_str();
    // We explicitly sync in add_changes, outside the lock.
    let fp = open_existing_synced_file(fpath.unwrap(), true);
    if fp.is_err() {
        let err = fp.unwrap_err();
        if !err.is_io_existing() {
            return Err(err);
        }
        // open exist Manifest
        let mt = TArcRW::new(RwLock::new(Manifest::new()));
        let (fp, net_creations) = mt.read().await.help_rewrite(dir).await?;
        assert_eq!(net_creations, 0);
        let mf = ManifestFile {
            fp: Some(fp),
            directory: dir.to_string(),
            deletions_rewrite_threshold: Default::default(),
            manifest: mt,
        };
        return Ok(mf);
    }
    let mut fp = fp.unwrap();
    let mut fp = tokio::fs::File::from_std(fp);
    let (mf, trunc_offset) = Manifest::replay_manifest_file(&mut fp).await?;
    // Truncate file so we don't have a half-written entry at the end.
    fp.set_len(trunc_offset as u64).await?;
    fp.seek(SeekFrom::Start(0)).await?;

    Ok(ManifestFile {
        fp: Some(fp),
        directory: dir.to_string(),
        deletions_rewrite_threshold: AtomicU32::new(deletions_threshold),
        manifest: Arc::new(RwLock::new(mf)),
    })
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
            op: CREATE,
        }
    }

    pub(crate) fn with_level(mut self, level: u32) -> Self {
        self.level = level;
        self
    }

    pub(crate) fn with_op(mut self, op: Operation) -> Self {
        self.op = op;
        self
    }

    pub(crate) fn build(self) -> ManifestChange {
        let mut mf = ManifestChange::default();
        mf.id = self.id;
        mf.level = self.level;
        mf.op = self.op;
        mf
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::ManifestChangeBuilder;
    use crate::pb::badgerpb3::mod_ManifestChange::Operation::{CREATE, DELETE};
    use crate::pb::badgerpb3::ManifestChange;
    use crate::test_util::{create_random_tmp_dir, random_tmp_dir};
    use tokio::io::AsyncWriteExt;
    use tracing::info;

    #[tokio::test]
    async fn t_manifest_file() {
        crate::test_util::tracing_log();
        let fpath = create_random_tmp_dir();
        info!("fpath => {}", fpath);
        let mut mf = super::ManifestFile::open_or_create_manifest_file(&fpath, 10)
            .await
            .unwrap();

        for i in 0..101 {
            mf.add_changes(vec![ManifestChangeBuilder::new(i + 1)
                .with_op(CREATE)
                .with_level(1)
                .build()])
                .await
                .unwrap();
        }

        for i in 0..100 {
            mf.add_changes(vec![ManifestChangeBuilder::new(i + 1)
                .with_op(DELETE)
                .build()])
                .await
                .unwrap();
        }
    }
}
