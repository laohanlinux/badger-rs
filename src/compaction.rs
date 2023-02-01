use crate::levels::CompactDef;
use crate::table::table::Table;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct CompactStatus {
    // every level has a *CompactionStatus* that includes multipart *KeyRange*
    levels: RwLock<Vec<LevelCompactStatus>>,
}

impl CompactStatus {
    // Check whether we can run this *CompactDef*. That it doesn't overlap with any
    // other running Compaction. If it can be run, it would store this run in the compactStatus state.
    pub(crate) fn compare_and_add(&self, cd: &CompactDef) -> bool {
        let level = cd.this_level.level();
        assert!(
            level < self.rl().len() - 1,
            "Got level {}, max level {}",
            level,
            self.rl().len()
        );
        let lc = self.levels.read();
        let this_level = lc.get(level).unwrap();
        let next_level = lc.get(level + 1).unwrap();
        if this_level.overlaps_with(&cd.this_range) {
            return false;
        }
        if next_level.overlaps_with(&cd.next_range) {
            return false;
        }

        // Check whether this level really needs compaction or not. Otherwise, we'll end up
        // running parallel compactions for the same level.
        // *NOTE*: We can directly call this_level.total_size, because we already have acquired a read lock
        // over this and the next level.
        if cd.this_level.get_total_size() - this_level.get_del_size()
            < cd.this_level.get_max_total_size()
        {
            return false;
        }
        this_level.ranges.write().push(cd.this_range.clone());
        next_level.ranges.write().push(cd.next_range.clone());
        this_level.incr_del_size(cd.this_size.load(Ordering::Relaxed));
        true
    }

    pub(crate) fn delete(&self, cd: &CompactDef) {
        let level = cd.this_level.level();
        let levels = self.wl();
        assert!(
            level < levels.len() - 1,
            "Got level {}, Max levels {}",
            level,
            levels.len()
        );
    }

    pub(crate) fn overlaps_with(&self, level: usize, this: &KeyRange) -> bool {
        let compact_status = self.wl();
        compact_status[level].overlaps_with(this)
    }

    // Return level's deleted data count
    pub(crate) fn del_size(&self, level: usize) -> u64 {
        let compact_status = self.rl();
        compact_status[level].get_del_size()
    }

    // Return Level's compaction status with *WriteLockGuard*
    fn wl(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<LevelCompactStatus>> {
        self.levels.write()
    }

    // Return Level's compaction status with *ReadLockGuard*
    fn rl(&self) -> RwLockReadGuard<'_, RawRwLock, Vec<LevelCompactStatus>> {
        self.levels.read()
    }
}

// Every level compacted status(ranges).
#[derive(Clone, Debug)]
pub(crate) struct LevelCompactStatus {
    ranges: Arc<RwLock<Vec<KeyRange>>>, // not any overlaps
    del_size: Arc<AtomicU64>,           // all KeyRange size
}

impl LevelCompactStatus {
    // returns true if self.ranges and dst has overlap, otherwise returns false
    fn overlaps_with(&self, dst: &KeyRange) -> bool {
        self.ranges.write().iter().any(|r| r.overlaps_with(dst))
    }

    // remove dst from self.ranges
    fn remove(&mut self, dst: &KeyRange) -> bool {
        let mut rlock = self.ranges.write();
        let len = rlock.len();
        rlock.retain(|r| r.equals(dst));
        len > rlock.len()
    }

    fn get_del_size(&self) -> u64 {
        self.del_size.load(Ordering::Relaxed)
    }

    fn incr_del_size(&self, n: u64) {
        self.del_size.fetch_add(n, Ordering::Relaxed);
    }

    fn decr_del_size(&self, n: u64) {
        self.del_size.fetch_sub(n, Ordering::Relaxed);
    }
}

// [left, right], Special inf is range all if it be set `true`
#[derive(Clone, Debug)]
pub(crate) struct KeyRange {
    pub(crate) left: Vec<u8>, // TODO zero Copy
    pub(crate) right: Vec<u8>,
    pub(crate) inf: bool,
}

impl Display for KeyRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[left={:?}, right={:?}, inf={}]",
            self.left, self.right, self.inf
        )
    }
}

// Including all keys
pub(crate) const INFO_RANGE: KeyRange = KeyRange {
    left: vec![],
    right: vec![],
    inf: true,
};

impl KeyRange {
    // Get the KeyRange of tables
    pub(crate) fn get_range(tables: &Vec<Table>) -> KeyRange {
        assert!(!tables.is_empty());
        let mut smallest = tables[0].smallest();
        let mut biggest = tables[0].biggest();
        for i in 1..tables.len() {
            if tables[i].smallest() < smallest {
                smallest = tables[i].smallest();
            }
            if tables[i].biggest() > biggest {
                biggest = tables[i].biggest();
            }
        }

        KeyRange {
            left: smallest.to_vec(),
            right: biggest.to_vec(),
            inf: false,
        }
    }

    // Left, right, inf all same, indicate equal
    pub(crate) fn equals(&self, other: &KeyRange) -> bool {
        self.left == other.left && self.right == self.right && self.inf == self.inf
    }

    // Check for overlap, *Notice*, if a and b are all inf, indicate has overlap.
    pub(crate) fn overlaps_with(&self, other: &KeyRange) -> bool {
        if self.inf || other.inf {
            return true;
        }

        if self.left > other.right {
            return false;
        }
        if self.right < other.left {
            return false;
        }
        true
    }
}
