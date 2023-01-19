use crate::compaction::KeyRange;
use crate::kv::{WeakKV, KV};
use crate::table::iterator::{IteratorImpl, IteratorItem};
use crate::table::table::Table;
use crate::types::{XArc, XWeak};
use crate::y::iterator::Xiterator;
use crate::Result;

use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;


pub(crate) type LevelHandler = XArc<LevelHandlerInner>;
pub(crate) type WeakLevelHandler = XWeak<LevelHandlerInner>;

impl From<LevelHandlerInner> for LevelHandler {
    fn from(value: LevelHandlerInner) -> Self {
        XArc::new(value)
    }
}

impl LevelHandler {
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

    /// init with tables
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
            // TODO avoid copy
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

    // Returns the tables that intersect with key range. Returns a half-interval.
    // This function should already have acquired a read lock, and this is so important the caller must
    // pass an empty parameter declaring such.
    // TODO Opz me
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
    fn replace_tables(&self, new_tables: Vec<Table>) -> Result<()> {
        // Need to re-search the range of tables in this level to be replaced as other goroutines might
        // be changing it as well. (They can't touch our tables, but if they add/remove other tables,
        // the indices get shifted around.)
        if new_tables.is_empty() {
            return Ok(());
        }
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
    pub(crate) fn try_add_level0_table(&self, t: Table) -> bool {
        assert_eq!(self.x.level.load(Ordering::Relaxed), 0);
        let tw = self.tables_wl();
        if tw.len() >= self.kv().x.opt.num_level_zero_tables_stall {
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

    // Acquires a read-lock to access s.tables. It return a list of table_handlers.
    pub(crate) fn get_table_for_key(&self, key: &[u8]) -> Option<IteratorItem> {
        return if self.x.level.load(Ordering::Relaxed) == 0 {
            let tw = self.tables_rd();
            for tb in tw.iter().rev() {
                tb.incr_ref();
                let it = IteratorImpl::new(tb.to_ref(), false);
                let item = it.seek(key);
                tb.decr_ref();
                if item.is_none() {
                    // todo add metrics
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
            let it = IteratorImpl::new(tb.to_ref(), false);
            let item = it.seek(key);
            tb.decr_ref();
            if item.is_none() {
                // todo add metrics
            }
            item
        };
    }

    pub(crate) fn level(&self) -> usize {
        self.x.level.load(Ordering::Relaxed) as usize
    }

    fn kv(&self) -> XArc<KV> {
        self.x.kv.upgrade().unwrap()
    }
}

pub(crate) struct LevelHandlerInner {
    // TODO this lock maybe global, not only for compacted
    pub(crate) self_lock: Arc<RwLock<()>>,
    // Guards tables, total_size.
    // For level >= 1, *tables* are sorted by key ranges, which do not overlap.
    // For level 0, *tables* are sorted by time.
    // For level 0, *newest* table are at the back. Compact the oldest one first, which is at the front.
    pub(crate) tables: Arc<RwLock<Vec<Table>>>,
    pub(crate) total_size: AtomicU64,
    // The following are initialized once and const.
    pub(crate) level: AtomicI32,
    str_level: Arc<String>,
    pub(crate) max_total_size: AtomicU64,
    kv: WeakKV,
}

impl LevelHandlerInner {
    pub(crate) fn new(kv: WeakKV, level: usize) -> LevelHandlerInner {
        LevelHandlerInner {
            self_lock: Arc::new(Default::default()),
            tables: Arc::new(Default::default()),
            total_size: Default::default(),
            level: Default::default(),
            str_level: Arc::new(format!("L{}", level)),
            max_total_size: Default::default(),
            kv,
        }
    }
}
