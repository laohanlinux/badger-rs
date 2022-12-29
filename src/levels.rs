use crate::compaction::CompactStatus;
use crate::kv::{WeakKV, KV};
use crate::level_handler::LevelHandlerInner;
use crate::manifest::Manifest;
use crate::table::table::new_file_name;
use crate::Error::Unexpected;
use crate::Result;
use awaitgroup::WaitGroup;
use log::{error, info};
use std::collections::HashSet;
use std::fs::remove_file;
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

pub(crate) struct LevelsController {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandlerInner>>,
    kv: *const KV,
    // Atomic
    next_file_id: AtomicI64,
    // For ending compactions.
    compact_worker_wg: WaitGroup,
    c_status: CompactStatus,
}

impl Default for LevelsController {
    fn default() -> Self {
        todo!()
    }
}

struct LevelsControllerInner {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandlerInner>>,
    kv: WeakKV,
    // Atomic
    next_file_id: AtomicI64,
    // For ending compactions.
    compact_worker_wg: WaitGroup,
    c_status: CompactStatus,
}

impl LevelsControllerInner {
    // Returns true if level zero may be compacted, without accounting for compactions that already
    // might be happening.
    fn is_level0_compact_table(&self) -> bool {
        // self.levels[0]
        todo!()
    }

    // Checks that all necessary table files exist and removes all table files not
    // referenced by the manifest. *ids* is a set of table file id's that were read from the directory
    // listing.
    fn revert_to_manifest(dir: &str, mf: Manifest, ids: &HashSet<u64>) -> Result<()> {
        // 1. check all files in manifest exists.
        for (id, _) in &mf.tables {
            if !ids.contains(id) {
                return Err(format!("file does not exist for table {}", id).into());
            }
        }
        // 2. delete files that shouldn't exist
        for id in ids {
            if !mf.tables.contains_key(id) {
                info!("Table file {} not referenced in MANIFEST", id);
                let file_name = new_file_name(*id, dir.clone().parse().unwrap());
                if let Err(err) = remove_file(file_name) {
                    error!("While removing table {}, err: {:?}", id, err);
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct CompactDef {}
