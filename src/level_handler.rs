use crate::compaction::KeyRange;
use crate::kv::{WeakKV, KV};
use crate::table::iterator::{ConcatIterator, IteratorImpl, IteratorItem};
use crate::table::table::{Table, TableCore};
use crate::types::{Channel, XArc, XWeak};
use crate::y::merge_iterator::MergeIterOverBuilder;
use crate::Result;
use core::slice::SlicePattern;

use crate::levels::CompactDef;
use crate::options::Options;
use crate::table::builder::Builder;
use drop_cell::defer;
use log::info;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

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

            if tables[j - 1].biggest() >= tables[j].smallest() {
                return Err(format!(
                    "Inter: {} vs {}: level={} j={} numTables={}",
                    String::from_utf8_lossy(tables[j - 1].biggest()),
                    String::from_utf8_lossy(tables[j].smallest()),
                    self.level(),
                    j,
                    num_tables
                )
                .into());
            }
            if tables[j].smallest() > tables[j].biggest() {
                return Err(format!(
                    "Intra: {} vs {}: level={} j={} numTables={}",
                    String::from_utf8_lossy(tables[j].smallest()),
                    String::from_utf8_lossy(tables[j].biggest()),
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
        self.get_total_size() - del_size >= self.get_max_total_size()
    }

    pub(crate) fn get_total_size(&self) -> u64 {
        self.x.total_size.load(Ordering::Relaxed)
    }

    pub(crate) fn get_max_total_size(&self) -> u64 {
        self.x.max_total_size.load(Ordering::Relaxed)
    }

    // delete current level's tables of to_del
    pub(crate) fn delete_tables(&self, to_del: Vec<u64>) {
        let to_del = to_del.iter().map(|id| *id).collect::<HashSet<_>>();
        let mut tb_wl = self.tables_wl();
        tb_wl.retain_mut(|tb| {
            if to_del.contains(&tb.x.id()) {
                // delete table reference
                tb.decr_ref();
                return false;
            }
            true
        });
    }

    // init with tables
    pub(crate) fn init_tables(&self, tables: Vec<Table>) {
        let total_size = tables.iter().fold(0, |acc, table| acc + table.size());
        self.x
            .total_size
            .store(total_size as u64, Ordering::Relaxed);
        let mut tb_wl = self.tables_wl();
        (*tb_wl) = tables;
        if self.x.level.load(Ordering::Relaxed) == 0 {
            // key range will overlap. Just sort by file_id in ascending order
            // because newer tables are at the end of level 0.
            tb_wl.sort_by_key(|tb| tb.x.id());
        } else {
            // Sort tables by keys.
            tb_wl.sort_by_key(|tb| tb.smallest().to_vec());
        }
    }

    // Get table write lock guards.
    fn tables_wl(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<Table>> {
        self.x.tables.write()
    }

    // Get table read lock guards
    fn tables_rd(&self) -> RwLockReadGuard<'_, RawRwLock, Vec<Table>> {
        self.x.tables.read()
    }

    // Returns the tables that intersect with key range. Returns a half-interval [left, right].
    // This function should already have acquired a read lock, and this is so important the caller must
    // pass an empty parameter declaring such.
    pub(crate) fn overlapping_tables(&self, key_range: &KeyRange) -> (usize, usize) {
        let left = self
            .tables_rd()
            .binary_search_by(|tb| key_range.left.as_slice().cmp(tb.biggest()));

        let right = self
            .tables_rd()
            .binary_search_by(|tb| key_range.right.as_slice().cmp(tb.smallest()));
        (left.unwrap(), right.unwrap())
    }

    pub(crate) fn get_total_siz(&self) -> u64 {
        self.x.total_size.load(Ordering::Relaxed)
    }

    // Replace tables[left:right] with new_tables, Note this EXCLUDES tables[right].
    // You must be call decr() to delete the old tables _after_ writing the update to the manifest.
    pub(crate) fn replace_tables(&self, new_tables: Vec<Table>) -> Result<()> {
        // Need to re-search the range of tables in this level to be replaced as other goroutines might
        // be changing it as well. (They can't touch our tables, but if they add/remove other tables,
        // the indices get shifted around.)
        if new_tables.is_empty() {
            return Ok(());
        }
        // TODO Add lock (think of level's sharing lock)
        // Increase total_size first.
        for tb in &new_tables {
            self.x
                .total_size
                .fetch_add(tb.size() as u64, Ordering::Relaxed);
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
            let mut tables_lck = self.tables_wl();
            tables_lck.retain_mut(|tb| {
                let left = tb.biggest() <= key_range.left.as_slice();
                let right = tb.smallest() > key_range.right.as_slice();
                if left || right {
                    return true;
                } else {
                    // TODO it should be not a good idea decr reference here, slow lock
                    // decr table reference
                    tb.decr_ref();
                    self.x
                        .total_size
                        .fetch_sub(tb.size() as u64, Ordering::Relaxed);
                    false
                }
            });
            tables_lck.extend(new_tables);
            // TODO avoid resort
            tables_lck.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        }
        Ok(())
    }

    // Return true if ok and no stalling.
    pub(crate) async fn try_add_level0_table(&self, t: Table) -> bool {
        assert_eq!(self.x.level.load(Ordering::Relaxed), 0);
        let tw = self.tables_wl();
        if tw.len() >= self.opt.num_level_zero_tables_stall {
            return false;
        }
        t.incr_ref();
        self.x
            .total_size
            .fetch_add(t.size() as u64, Ordering::Relaxed);
        self.tables_wl().push(t);
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
        return if self.x.level.load(Ordering::Relaxed) == 0 {
            let tw = self.tables_rd();
            for tb in tw.iter().rev() {
                tb.incr_ref();
                let it = IteratorImpl::new(tb.clone(), false);
                let item = it.seek(key);
                tb.decr_ref();
                if item.is_none() {
                    // todo add metrics
                } else {
                    return item;
                }
            }
            None
        } else {
            let tw = self.tables_rd();
            let ok = tw.binary_search_by(|tb| tb.biggest().cmp(key));
            if ok.is_err() {
                // todo add metrics
                return None;
            }
            let tb = tw.get(ok.unwrap()).unwrap();
            tb.incr_ref();
            let it = IteratorImpl::new(tb.clone(), false);
            let item = it.seek(key);
            tb.decr_ref();
            if item.is_none() {
                // todo add metrics
            }
            item
        };
    }

    // returns current level
    pub(crate) fn level(&self) -> usize {
        self.x.level.load(Ordering::Relaxed) as usize
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
    pub(crate) level: AtomicI32,
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
            level: Default::default(),
            str_level: Arc::new(format!("L{}", level)),
            max_total_size: Default::default(),
            opt,
        }
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
