use crate::kv::WeakKV;
use crate::table::table::TableCore;
use crate::types::{XArc, XWeak};
use crate::Result;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

pub(crate) type LevelHandler = XArc<LevelHandlerInner>;
pub(crate) type WeakLevelHandler = XWeak<LevelHandlerInner>;

pub(crate) struct LevelHandlerInner {
    // Guards tables, total_size.
    // For level >= 1, *tables* are sorted by key ranges, which do not overlap.
    // For level 0, *tables* are sorted by time.
    // For level 0, *newest* table are at the back. Compact the oldest one first, which is at the front.
    tables: Arc<RwLock<Vec<TableCore>>>,
    total_size: AtomicU64,
    // The following are initialized once and const.
    level: AtomicI32,
    str_level: Arc<String>,
    max_total_size: Arc<i64>,
    kv: WeakKV,
}

impl LevelHandlerInner {
    fn init_tables(&self, tables: Vec<TableCore>) {
        let total_size = tables.iter().fold(0, |acc, table| acc + table.size());
        self.total_size.store(total_size as u64, Ordering::Relaxed);
        let mut tb_wl = self.tables_wl();
        (*tb_wl) = tables;
        if self.level.load(Ordering::Relaxed) == 0 {
            // key range will overlap. Just sort by file_id in ascending order
            // because newer tables are at the end of level 0.
            tb_wl.sort_by_key(|tb| tb.id());
        } else {
            // Sort tables by keys.
            // TODO avoid copy
            tb_wl.sort_by_key(|tb| tb.smallest().to_vec());
        }
    }

    // TODO add deference table deleted
    fn delete_tables(&self, to_del: Vec<u64>) {
        let to_del = to_del.iter().map(|id| *id).collect::<HashSet<_>>();
        let mut tb_wl = self.tables_wl();
        tb_wl.retain(|tb| !to_del.contains(&tb.id()));
    }

    // Replace tables[left:right] with new_tables, Note this EXCLUDES tables[right].
    // You must be call decr() to delete the old tables _after_ writing the update to the manifest.
    fn replace_tables(&self, new_tables: Vec<TableCore>) -> Result<()> {
        // Need to re-search the range of tables in this level to be replaced as other goroutines might
        // be changing it as well. (They can't touch our tables, but if they add/remove other tables,
        // the indices get shifted around.)
        if new_tables.is_empty() {
            return Ok(());
        }

        Ok(())
    }

    fn overlapping_tables(&self) {}

    fn get_total_siz(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    fn tables_wl(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<TableCore>> {
        self.tables.write()
    }
    fn tables_rd(&self) -> RwLockReadGuard<'_, RawRwLock, Vec<TableCore>> {
        self.tables.read()
    }
}
