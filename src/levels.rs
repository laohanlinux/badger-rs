use crate::compaction::{CompactStatus, KeyRange, INFO_RANGE};
use crate::kv::{ArcKV, WeakKV, KV};
use crate::level_handler::{LevelHandler, LevelHandlerInner, WeakLevelHandler};
use crate::manifest::Manifest;
use crate::options::Options;
use crate::table::table::{new_file_name, Table, TableCore};
use crate::types::{Closer, XArc, XWeak};
use crate::Error::Unexpected;
use crate::Result;
use atomic::Ordering;
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
use std::time::{Duration, SystemTime};
use tokio::macros::support::thread_rng_n;

#[derive(Clone)]
pub(crate) struct LevelsController {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandler>>,
    kv: WeakKV,
    next_file_id: Arc<AtomicI64>,
    // For ending compactions.
    compact_worker_wg: Arc<WaitGroup>,
    c_status: Arc<CompactStatus>,
}

unsafe impl Sync for LevelsController {}

unsafe impl Send for LevelsController {}

impl Default for LevelsController {
    fn default() -> Self {
        todo!()
    }
}

impl LevelsController {
    fn new(kv: ArcKV, mf: &Manifest) -> Result<LevelsController> {
        assert!(kv.x.opt.num_level_zero_tables_stall > kv.x.opt.num_level_zero_tables);
        let mut levels = vec![];
        for i in 0..kv.x.opt.max_levels {
            let lh = LevelHandlerInner::new(WeakKV::from(&kv), i);
            levels.push(LevelHandler::from(lh));
            if i == 0 {
            } else if i == 1 {
            } else {
            }
        }
        todo!()
    }

    fn cleanup_levels(&self) -> Result<()> {
        for level in self.levels.iter() {
            level.close()?;
        }
        Ok(())
    }

    // start compact
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
        {
            let duration = thread_rng_n(1000);
            tokio::time::sleep(Duration::from_millis(duration as u64)).await;
        }
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            // why interval can life long
            let done = lc.has_been_closed();
            tokio::select! {
                _ = interval.tick() => {
                    let pick: Vec<CompactionPriority> = self.pick_compact_levels();
                    for p in pick {
                        match self.do_compact(p) {
                            Ok(true) => {
                                info!("succeed to compacted")
                            },
                            Ok(false) => {
                                info!("failed to do compacted");
                                break;
                            },
                            Err(err) => { // TODO handle error
                                error!("failed to do compacted, {:?}", err);
                            },
                        }
                    }
                },
                _ =  done.recv() => {
                    info!("closing compact job");
                    return;
                }
            }
        }
    }

    // Picks some table on level l and compacts it away to the next level.
    fn do_compact(&self, p: CompactionPriority) -> Result<bool> {
        let l = p.level;
        assert!(l + 1 < self.must_kv().opt.max_levels); //  Sanity check.
        let mut cd = CompactDef::default();
        cd.this_level = (self.levels[l]).clone();
        cd.next_level = (self.levels[l + 1]).clone();
        info!("Got compaction priority: {:?}", p);
        // While picking tables to be compacted, both level's tables are expected to
        // remain unchanged.
        if l == 0 {
            if !self.fill_tables_l0(&mut cd) {
                info!("failed to fill tables for level {}", l);
                return Ok(false);
            }
        } else {
            if !self.fill_tables(&mut cd) {
                info!("failed to fill tables for level {}", l);
                return Ok(false);
            }
        }
        info!("Running for level: {}", cd.this_level.level());
        info!("{:?}", self.c_status);

        info!("Compaction for level: {} DONE", cd.this_level.level());
        Ok(true)
    }

    fn run_compact_def(&self, l: usize, cd: &mut CompactDef) -> Result<()> {
        let time_start = SystemTime::now();
        let this_level = cd.this_level.clone();
        let next_level = cd.next_level.clone();

        if this_level.level() >= 1 && cd.bot.is_empty() {
            assert_eq!(cd.top.len(), 1);
        }
        todo!()
    }

    fn fill_tables_l0(&self, cd: &mut CompactDef) -> bool {
        cd.lock_levels();
        let top = cd.this_level.to_ref().tables.read();
        // TODO here maybe have some issue that i don't understand
        let tables = top.to_vec();
        cd.top.extend(tables);
        if cd.top.is_empty() {
            cd.unlock_levels();
            return false;
        }
        cd.this_range = INFO_RANGE;
        let kr = KeyRange::get_range(cd.top.as_ref());
        let (left, right) = cd.next_level.overlapping_tables(&kr);
        let bot = cd.next_level.to_ref().tables.read();
        let tables = bot.to_vec();
        cd.bot.extend(tables[left..right].to_vec());
        if cd.bot.is_empty() {
            cd.next_range = kr;
        } else {
            cd.next_range = KeyRange::get_range(cd.bot.as_ref());
        }
        // if !self.c_status.
        cd.unlock_levels();
        true
    }

    fn fill_tables(&self, cd: &mut CompactDef) -> bool {
        cd.lock_levels();
        let mut tables = cd.this_level.to_ref().tables.read().to_vec();
        if tables.is_empty() {
            cd.unlock_levels();
            return false;
        }
        // Find the biggest table, and compact taht first.
        // TODO: Try other table picking strategies.
        tables.sort_by(|a, b| b.size().cmp(&a.size()));
        for t in tables {
            cd.this_size.store(t.size() as u64, Ordering::Relaxed);
            cd.this_range = KeyRange {
                left: t.smallest().to_vec(),
                right: t.biggest().to_vec(),
                inf: false,
            };
            if self
                .c_status
                .overlaps_with(cd.this_level.level(), &cd.this_range)
            {
                continue;
            }

            cd.top.clear();
            cd.top.push(t);
            let (left, right) = cd.next_level.overlapping_tables(&cd.this_range);
            let bot = cd.next_level.to_ref().tables.read();
            let tables = bot.to_vec();
            cd.bot.clear();
            cd.bot.extend(tables[left..right].to_vec());

            if cd.bot.is_empty() {
                cd.bot.clear();
                cd.next_range = cd.this_range.clone();
                if !self.c_status.compare_and_add(cd) {
                    continue;
                }
                cd.unlock_levels();
                return true;
            }

            cd.next_range = KeyRange::get_range(cd.bot.as_ref());

            if self
                .c_status
                .overlaps_with(cd.next_level.level(), &cd.next_range)
            {
                continue;
            }

            if !self.c_status.compare_and_add(&cd) {
                continue;
            }
            cd.unlock_levels();
            return true;
        }
        cd.unlock_levels();
        false
    }

    // Determines which level to compact.
    // Base on https://github.com/facebook/rocksdb/wiki/Leveled-Compaction.
    fn pick_compact_levels(&self) -> Vec<CompactionPriority> {
        // This function must use identical criteria for guaranteeing compaction's progress that
        // add_level0_table use.

        let mut prios = vec![];
        // cstatus is checked to see if level 0's tables are already being compacted.
        // *NOTICE* level 0 only has one compact job
        if !self.c_status.overlaps_with(0, &INFO_RANGE) && self.is_level0_compactable() {
            prios.push(CompactionPriority {
                level: 0,
                score: (self.levels[0].num_tables() as f64)
                    / (self.must_kv().opt.num_level_zero_tables as f64),
            })
        }

        // stats level 1..n
        for (i, level) in self.levels[1..].iter().enumerate() {
            // Don't consider those tables that are already being compacted right now.
            let del_size = self.c_status.del_size(i + 1);
            if level.is_compactable(del_size) {
                prios.push(CompactionPriority {
                    level: i + 1,
                    score: ((level.get_total_size() - del_size) as f64
                        / level.get_max_total_size() as f64),
                });
            }
        }
        // sort from big to small.
        prios.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        prios
    }

    // Return true if level zero may be compacted, without accounting for compactions that already
    // might be happening.
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

pub(crate) struct CompactDef {
    pub(crate) this_level: LevelHandler,
    pub(crate) next_level: LevelHandler,
    pub(crate) top: Vec<Table>,
    pub(crate) bot: Vec<Table>,
    pub(crate) this_range: KeyRange,
    pub(crate) next_range: KeyRange,
    pub(crate) this_size: AtomicU64,
}

impl Default for CompactDef {
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

impl CompactDef {
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
