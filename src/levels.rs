use crate::compaction::{CompactStatus, KeyRange, INFO_RANGE};
use crate::kv::{WeakKV, KV};
use crate::level_handler::{LevelHandler, LevelHandlerInner, WeakLevelHandler};
use crate::manifest::Manifest;
use crate::options::Options;
use crate::table::table::{new_file_name, Table, TableCore};
use crate::types::{Closer, XArc, XWeak};
use crate::Error::Unexpected;
use crate::Result;
use awaitgroup::WaitGroup;
use log::{error, info};
use parking_lot::lock_api::RawRwLock;
use parking_lot::{RwLock, RwLockReadGuard};
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::remove_file;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct LevelsController {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandler>>,
    kv: WeakKV,
    // Atomic
    next_file_id: Arc<AtomicI64>,
    // For ending compactions.
    compact_worker_wg: Arc<WaitGroup>,
    c_status: CompactStatus,
}

unsafe impl Sync for LevelsController {}

unsafe impl Send for LevelsController {}

impl Default for LevelsController {
    fn default() -> Self {
        todo!()
    }
}

impl LevelsController {
    fn cleanup_levels(&self) -> Result<()> {
        for level in self.levels.iter() {
            level.close()?;
        }
        Ok(())
    }

    fn start_compact(&self, lc: Closer) {
        for i in 0..self.must_kv().opt.num_compactors {
            lc.add_running(1);
            let lc = lc.clone();
            let _self = self.clone();
            tokio::spawn(async move {
                _self.run_worker(lc).await;
            });
        }
    }
    async fn run_worker(&self, lc: Closer) {
        if self.must_kv().opt.do_not_compact {
            lc.done();
            return;
        }
        lc.done();
        // add random time
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            // why interval can life long
            let done = lc.has_been_closed();
            tokio::select! {
                _ = interval.tick() => {
                    let pick: Vec<CompactionPriority> = self.pick_compact_levels();
                    for p in pick.iter() {

                    }
                },
                _ =  done.recv() => {return;}
            }
        }
    }
    // Picks some table on level l and compacts it away to the next level.
    fn do_compact(&self, p: CompactionPriority) -> Result<bool> {
        let level = p.level;
        assert!(level + 1 < self.must_kv().opt.max_levels); //  Sanity check.
        let mut cd = CompactDef::default();
        cd.this_level = (self.levels[level]).clone();
        cd.next_level = (self.levels[level + 1]).clone();
        info!("Got compaction priority: {:?}", p);
        // While picking tables to be compacted, both level's tables are expected to
        // remain unchanged.
        if level == 0 {}
        Ok(true)
    }

    fn fill_tables_l0(&self, cd: &mut CompactDef) -> bool {
        cd.lock_levels();
        let top = cd.this_level.to_ref().tables.read();
        // TODO here maybe have some issue that i don't understand
        let tables = top.to_vec();
        cd.top.borrow_mut().extend(tables);
        if cd.top.borrow().is_empty() {
            cd.unlock_levels();
            return false;
        }
        cd.this_range = INFO_RANGE;
        let kr = KeyRange::get_range(cd.top.borrow().as_ref());
        let (left, right) = cd.next_level.overlapping_tables(&kr);
        let bot = cd.next_level.to_ref().tables.read();
        let tables = bot.to_vec();
        cd.bot.borrow_mut().extend(tables[left..right].to_vec());
        if cd.bot.borrow().is_empty() {
            cd.next_range = kr;
        } else {
            cd.next_range = KeyRange::get_range(cd.bot.borrow().as_ref());
        }
        // if !self.c_status.
        cd.unlock_levels();
        true
    }

    // Determines which level to compact.
    // Base on: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction.
    fn pick_compact_levels(&self) -> Vec<CompactionPriority> {
        // This function must use identical criteria for guaranteeing compaction's progress that
        // add_level0_table use.

        let mut prios = vec![];
        // cstatus is checked to see if level 0's tables are already being compacted.
        if !self.c_status.overlaps_with(0, &INFO_RANGE) && self.is_level0_compactable() {
            prios.push(CompactionPriority {
                level: 0,
                score: (self.levels[0].num_tables() as f64)
                    / (self.must_kv().opt.num_level_zero_tables as f64),
            })
        }

        for (i, level) in self.levels[1..].iter().enumerate() {
            // Don't consider those tables that are already being compacted right now.
            let del_size = self.c_status.del_size(i + 1);
            if level.is_compactable(del_size) {
                prios.push(CompactionPriority {
                    level: i + 1,
                    score: (level.get_total_size() as f64 / level.get_max_total_size() as f64),
                });
            }
        }
        prios.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        prios
    }

    fn is_level0_compactable(&self) -> bool {
        self.levels[0].num_tables() >= self.must_kv().opt.num_level_zero_tables
    }

    fn must_kv(&self) -> Arc<KV> {
        self.kv.x.upgrade().unwrap()
    }
}

#[derive(Debug, Clone)]
struct CompactionPriority {
    level: usize,
    score: f64,
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

pub(crate) struct CompactDef<'a> {
    pub(crate) this_level: LevelHandler,
    pub(crate) next_level: LevelHandler,
    top: RefCell<Vec<Table>>,
    bot: RefCell<Vec<Table>>,
    _c: RefCell<RwLockReadGuard<'a, Vec<Table>>>,
    this_range: KeyRange,
    next_range: KeyRange,
    this_size: AtomicU64,
}

impl<'a> Default for CompactDef<'a> {
    fn default() -> Self {
        // CompactDef {
        //     this_level: XWeak::new(),
        //     next_level: XWeak::new(),
        //     top: RwLockReadGuard::,
        //     bot: RwLock::new(vec![]),
        //     this_range: KeyRange {
        //         left: vec![],
        //         right: vec![],
        //         inf: false,
        //     },
        //     next_range: KeyRange {
        //         left: vec![],
        //         right: vec![],
        //         inf: false,
        //     },
        //     this_size: Default::default(),
        // }
        todo!()
    }
}

impl<'a> CompactDef<'a> {
    fn lock_levels(&self) {
        unsafe {
            self.this_level.x.self_lock.raw().lock_shared();
            self.next_level.x.self_lock.raw().lock_shared();
        }
    }

    fn unlock_levels(&self) {
        unsafe {
            self.next_level.x.self_lock.raw().unlock_shared();
            self.this_level.x.self_lock.raw().unlock_shared();
        }
    }
}
