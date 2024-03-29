use crate::compaction::KeyRange;

use crate::table::iterator::{IteratorImpl, IteratorItem};
use crate::table::table::Table;
use crate::types::XArc;

use crate::{event, hex_str, Result};
use core::slice::SlicePattern;
use std::fmt::Display;

use crate::options::Options;

use drop_cell::defer;
use log::{debug, info, warn};
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::collections::HashSet;

use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) type LevelHandler = XArc<LevelHandlerInner>;

impl From<LevelHandlerInner> for LevelHandler {
    fn from(value: LevelHandlerInner) -> Self {
        XArc::new(value)
    }
}

impl LevelHandler {
    // Check does some sanity check on one level of data or in-memory index.
    pub(crate) fn validate(&self) -> Result<()> {
        self.lock_exclusive();
        defer! {self.unlock_exclusive();}
        if self.level() == 0 {
            return Ok(());
        }
        let tables = self.tables.write();
        let num_tables = tables.len();
        for j in 1..num_tables {
            if j >= tables.len() {
                return Err(format!(
                    "Level={}, j={}, number_tables={}",
                    self.level(),
                    j,
                    num_tables
                )
                .into());
            }

            // overlap occurs
            if tables[j - 1].biggest() >= tables[j].smallest() {
                return Err(format!(
                    "Inter: {} vs {}: level={} j={} numTables={}",
                    hex_str(tables[j - 1].biggest()),
                    hex_str(tables[j].smallest()),
                    self.level(),
                    j,
                    num_tables
                )
                .into());
            }
            if tables[j].smallest() > tables[j].biggest() {
                return Err(format!(
                    "Intra: {} vs {}: level={} j={} numTables={}",
                    hex_str(tables[j].smallest()),
                    hex_str(tables[j].biggest()),
                    self.level(),
                    j,
                    num_tables
                )
                .into());
            }
        }

        Ok(())
    }

    // Returns true if the non-zero level may be compacted. *del_size* provides the size of the tables
    // which are currently being compacted so that we treat them as already having started being
    // compacted (because they have been, yet their size is already counted in get_total_size).
    pub(crate) fn is_compactable(&self, del_size: u64) -> bool {
        let compactable = self.get_total_size() - del_size >= self.get_max_total_size();

        #[cfg(test)]
        debug!(
            "trace level{}, does it compactable, total_size:{}, del_size:{}, max_size:{}, yes: {}",
            self.level(),
            self.get_total_size(),
            del_size,
            self.get_max_total_size(),
            compactable,
        );

        compactable
    }

    pub(crate) fn get_total_size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    pub(crate) fn incr_total_size(&self, n: u64) {
        let old = self.total_size.fetch_add(n, Ordering::Relaxed);
        #[cfg(test)]
        info!(
            "incr level{} total size: {} => {}",
            self.level(),
            old,
            self.get_total_size()
        );
    }

    pub(crate) fn decr_total_size(&self, n: u64) {
        let old = self.total_size.fetch_sub(n, Ordering::Relaxed);
        #[cfg(test)]
        info!(
            "decr level{} total size: {} => {}",
            self.level(),
            old,
            self.get_total_size()
        );
    }

    pub(crate) fn get_max_total_size(&self) -> u64 {
        self.max_total_size.load(Ordering::Relaxed)
    }

    // delete current level's tables of to_del
    pub(crate) fn delete_tables(&self, to_del: Vec<u64>) {
        let to_del_set = to_del.iter().map(|id| *id).collect::<HashSet<_>>();
        let level = self.level();
        let mut tb_wl = self.tables_wl();
        let before_tids = tb_wl.iter().map(|tb| tb.id()).collect::<Vec<_>>();
        {
            tb_wl.retain_mut(|tb| {
                if to_del_set.contains(&tb.id()) {
                    // delete table reference
                    tb.decr_ref();
                    self.decr_total_size(tb.size() as u64);
                    return false;
                }
                true
            });
        }
        let after_tids = tb_wl.iter().map(|tb| tb.id()).collect::<Vec<_>>();
        warn!(
            "after delete tables level:{},  {:?} => {:?}, to_del: {:?}",
            level, before_tids, after_tids, to_del,
        );
    }

    // init with tables
    pub(crate) fn init_tables(&self, tables: Vec<Table>) {
        let total_size = tables.iter().fold(0, |acc, table| acc + table.size());
        self.total_size.store(total_size as u64, Ordering::Relaxed);
        let mut tb_wl = self.tables_wl();
        (*tb_wl) = tables;
        if self.level() == 0 {
            // key range will overlap. Just sort by file_id in ascending order
            // because newer tables are at the end of level 0.
            tb_wl.sort_by_key(|tb| tb.id());
        } else {
            // Sort tables by keys.
            tb_wl.sort_by_key(|tb| tb.smallest().to_vec());
        }
    }

    // Get table write lock guards.
    fn tables_wl(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<Table>> {
        self.tables.write()
    }

    // Get table read lock guards
    fn tables_rd(&self) -> RwLockReadGuard<'_, RawRwLock, Vec<Table>> {
        self.tables.read()
    }

    pub(crate) fn debug_tables(&self) {
        let tw = self.tables_rd();
        info!(
            "=============debug tables, level: {}=====================",
            self.level()
        );
        for tb in tw.iter() {
            info!(
                "|tid:{}, smallest:{}, biggest:{}, size: {}|",
                tb.id(),
                hex_str(tb.smallest()),
                hex_str(tb.biggest()),
                tb.size(),
            );
        }
        info!("------------------------end-----------------------------");
    }

    // Returns the tables that intersect with key range. Returns a half-interval [left, right).
    // This function should already have acquired a read lock, and this is so important the caller must
    // pass an empty parameter declaring such.
    pub(crate) fn overlapping_tables(&self, key_range: &KeyRange) -> (usize, usize) {
        // probe.biggest() >= left
        let left = self
            .tables_rd()
            .binary_search_by(|probe| probe.biggest().cmp(&key_range.left));
        let right = self
            .tables_rd()
            .binary_search_by(|probe| probe.smallest().cmp(&key_range.right));

        info!(
            "overlapping tables, range: {}, left: {:?}, right: {:?}",
            key_range, left, right
        );
        let left = left.unwrap_or_else(|n| n);
        let right = right.map(|n| n + 1).unwrap_or_else(|n| n);
        if left == right {
            // simple handle
            return (0, 0);
        }
        (left, right)
    }

    pub(crate) fn get_total_siz(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    // Replace tables[left:right] with new_tables, Note this EXCLUDES tables[right].
    // You must be call decr() to delete the old tables _after_ writing the update to the manifest.
    pub(crate) fn replace_tables(&self, new_tables: Vec<Table>) -> Result<()> {
        // Need to re-search the range of tables in this level to be replaced as other goroutines might
        // be changing it as well. (They can't touch our tables, but if they add/remove other tables,
        // the indices get shifted around.)
        if new_tables.is_empty() {
            info!("No tables need to replace");
            return Ok(());
        }
        // TODO Add lock (think of level's sharing lock)
        // Increase total_size first.
        for tb in &new_tables {
            self.incr_total_size(tb.size() as u64);
            // add table reference
            tb.incr_ref();
        }
        let key_range = KeyRange {
            left: new_tables.first().unwrap().smallest().to_vec(),
            right: new_tables.last().unwrap().biggest().to_vec(),
            inf: false,
        };

        // TODO Opz code
        {
            let level_id = self.level();
            let mut tables_lck = self.tables_wl();
            let old_ids = tables_lck.iter().map(|tb| tb.id()).collect::<Vec<_>>();
            // TODO FIXME may be it is error.
            tables_lck.retain_mut(|tb| {
                let left = tb.biggest() <= key_range.left.as_slice();
                let right = tb.smallest() > key_range.right.as_slice();
                if left || right {
                    return true;
                } else {
                    // TODO it should be not a good idea decr reference here, slow lock
                    // decr table reference
                    tb.decr_ref();
                    self.decr_total_size(tb.size() as u64);
                    false
                }
            });
            let will_add = new_tables.iter().map(|tb| tb.id()).collect::<Vec<_>>();
            tables_lck.extend(new_tables);
            // TODO avoid resort
            tables_lck.sort_by(|a, b| a.smallest().cmp(b.smallest()));

            let new_ids = tables_lck.iter().map(|tb| tb.id()).collect::<Vec<_>>();
            info!(
                "after replace tables, level:{}, will_add:{:?}, {:?} => {:?}",
                level_id, will_add, old_ids, new_ids
            );
        }
        Ok(())
    }

    // Return true if ok and no stalling that will hold a new table reference
    pub(crate) async fn try_add_level0_table(&self, t: Table) -> bool {
        assert_eq!(self.get_level(), 0);
        let mut tw = self.tables_wl();
        if tw.len() >= self.opt.num_level_zero_tables_stall {
            // Too many tables at zero level need compact
            return false;
        }
        t.incr_ref();
        self.incr_total_size(t.size() as u64);
        tw.push(t);
        true
    }

    pub(crate) fn num_tables(&self) -> usize {
        self.tables_rd().len()
    }

    // Must be call only once
    pub(crate) fn close(&self) -> Result<()> {
        let tw = self.tables_wl();
        tw.iter().for_each(|tb| tb.decr_ref());
        Ok(())
    }

    // Acquires a read-lock to access s.tables. It returns a list of table_handlers.
    pub(crate) fn get_table_for_key(&self, key: &[u8]) -> Option<IteratorItem> {
        return if self.get_level() == 0 {
            // For level 0, we need to check every table. Remember to make a copy as self.tables may change
            // once we exit this function, and we don't want to lock the self.tables while seeking in tabbles.
            // CAUTION: Reverse the tables.
            let tw = self.tables_rd();
            for tb in tw.iter().rev() {
                tb.incr_ref();
                // check it by bloom filter
                if tb.does_not_have(key) {
                    //debug!("not contain it, key #{}, st: {}", hex_str(key), tb.id());
                    event::get_metrics().num_lsm_bloom_hits.inc();
                    tb.decr_ref();
                    continue;
                }
                event::get_metrics().num_lsm_gets.inc();
                let it = IteratorImpl::new(tb.clone(), false);
                let item = it.seek(key);
                tb.decr_ref();
                if let Some(item) = item {
                    if item.key() != key {
                        continue;
                    }
                    return Some(item);
                }
            }
            None
        } else {
            //self.debug_tables();
            let tw = self.tables_rd();
            let ok = tw.binary_search_by(|tb| tb.biggest().cmp(key));
            // #[cfg(test)]
            // info!("find key #{} at level{}, {:?}", hex_str(key), self.level(), ok.unwrap_or_else(|n| n));

            let index = ok.unwrap_or_else(|n| n);
            if index >= tw.len() {
                // todo add metrics
                return None;
            }
            let tb = tw.get(index).unwrap();
            tb.incr_ref();
            if tb.does_not_have(key) {
                //debug!("not contain it, key #{}, st: {}", hex_str(key), tb.id());
                event::get_metrics().num_lsm_bloom_hits.inc();
                tb.decr_ref();
                return None;
            }
            event::get_metrics().num_lsm_gets.inc();
            let it = IteratorImpl::new(tb.clone(), false);
            let item = it.seek(key);
            tb.decr_ref();
            if let Some(item) = item {
                if item.key() == key {
                    return Some(item);
                }
            }
            return None;
        };
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<IteratorItem> {
        self.get_table_for_key(key)
    }

    // returns current level
    pub(crate) fn level(&self) -> usize {
        self.level.load(Ordering::Relaxed) as usize
    }

    pub(crate) fn to_log(&self) -> String {
        format!("{}", self)
    }
}

impl Display for LevelHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LevelHandler")
            .field("level", &self.get_level())
            .field("max", &self.max_total_size.load(Ordering::Relaxed))
            .field(
                "tables",
                &self
                    .tables_rd()
                    .iter()
                    .map(|tb| tb.id())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

pub(crate) struct LevelHandlerInner {
    // TODO this lock maybe global, not only for compacted
    pub(crate) self_lock: Arc<RwLock<()>>,
    // Guards tables, total_size.
    // For level >= 1, *tables* are sorted by key ranges, which do not overlap.
    // For level 0, *tables* are sorted by time.
    // For level 0, *newest* table are at the back. Compact the oldest one first, which is at the front.
    // TODO tables and total_size maybe should be lock with same lock.
    pub(crate) tables: Arc<RwLock<Vec<Table>>>,
    pub(crate) total_size: AtomicU64,
    // The following are initialized once and const.
    pub(crate) level: AtomicUsize,
    str_level: Arc<String>,
    pub(crate) max_total_size: AtomicU64,
    opt: Options,
}

impl LevelHandlerInner {
    pub(crate) fn new(opt: Options, level: usize) -> LevelHandlerInner {
        LevelHandlerInner {
            self_lock: Arc::new(Default::default()),
            tables: Arc::new(Default::default()),
            total_size: Default::default(),
            level: AtomicUsize::new(level),
            str_level: Arc::new(format!("L{}", level)),
            max_total_size: Default::default(),
            opt,
        }
    }

    #[inline]
    pub(crate) fn get_level(&self) -> usize {
        self.level.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn lock_shared(&self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.self_lock.raw().lock_shared() }
    }

    #[inline]
    pub(crate) fn try_lock_share(&self) -> bool {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.self_lock.raw().try_lock_shared() }
    }

    #[inline]
    pub(crate) fn unlock_shared(&self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.self_lock.raw().unlock_shared() }
    }

    #[inline]
    pub(crate) fn lock_exclusive(&self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.self_lock.raw().lock_exclusive() }
    }

    #[inline]
    pub(crate) fn try_lock_exclusive(&self) -> bool {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.self_lock.raw().try_lock_exclusive() }
    }

    #[inline]
    pub(crate) fn unlock_exclusive(&self) {
        use parking_lot::lock_api::RawRwLock;
        unsafe { self.self_lock.raw().unlock_exclusive() }
    }
}

#[test]
fn raw_lock() {
    let lock = LevelHandlerInner::new(Options::default(), 10);
    lock.lock_shared();
    lock.lock_shared();
    assert_eq!(false, lock.try_lock_exclusive());
    lock.unlock_shared();
    lock.unlock_shared();

    assert_eq!(true, lock.try_lock_exclusive());
    assert_eq!(false, lock.try_lock_share());
    lock.unlock_exclusive();
    assert_eq!(true, lock.try_lock_share());
}
