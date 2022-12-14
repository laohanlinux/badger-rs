use crate::levels::CompactDef;
use crate::table::table::{Table, TableCore};
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::fmt::{Display, Formatter};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLockMappedWriteGuard;

#[derive(Clone)]
pub(crate) struct CompactStatus {
    levels: Arc<RwLock<Vec<LevelCompactStatus>>>,
}

impl CompactStatus {
    fn to_log(&self) {
        todo!()
    }

    // Check whether we can run this *CompactDef*. That it doesn't overlap with any
    // other running Compaction. If it can be run, it would store this run in the compactStatus state.
    pub(crate) fn compare_and_add(&self, cd: &mut CompactDef) -> bool {
        let level = cd.this_level.level();
        assert!(
            level < self.rl().len() - 1,
            "Got level {}, max level {}",
            level,
            self.rl().len()
        );

        let mut this_level =
            RwLockWriteGuard::map(self.levels.write(), |lc| lc.get_mut(level).unwrap());
        let mut next_level =
            RwLockWriteGuard::map(self.levels.write(), |lc| lc.get_mut(level + 1).unwrap());

        if this_level.overlaps_with(&cd.this_range) {
            return false;
        }
        if next_level.overlaps_with(&cd.next_range) {
            return false;
        }

        // Check whether this level really needs compaction or not. Otherwise, we'll end up
        // running parallel compactions for the same level.
        // *NOTE*: We can directly call this_level.total_size, because we already have acquire a read lock
        // over this and the next level.
        if cd.this_level.get_total_size() - this_level.del_size < cd.this_level.get_max_total_size()
        {
            return false;
        }
        this_level.ranges.push(cd.this_range.clone());
        next_level.ranges.push(cd.next_range.clone());
        this_level.del_size += cd.this_size.load(Ordering::Relaxed);
        true
    }

    pub(crate) fn overlaps_with(&self, level: usize, this: &KeyRange) -> bool {
        let compact_status = self.wl();
        compact_status[level].overlaps_with(this)
    }

    pub(crate) fn del_size(&self, level: usize) -> u64 {
        let compact_status = self.rl();
        compact_status[level].del_size
    }

    fn wl(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<LevelCompactStatus>> {
        self.levels.write()
    }
    fn rl(&self) -> RwLockReadGuard<'_, RawRwLock, Vec<LevelCompactStatus>> {
        self.levels.read()
    }
}

#[derive(Debug)]
pub(crate) struct LevelCompactStatus {
    ranges: Vec<KeyRange>,
    del_size: u64,
}

impl LevelCompactStatus {
    fn overlaps_with(&self, dst: &KeyRange) -> bool {
        self.ranges.iter().any(|r| r.overlaps_with(dst))
    }

    fn remove(&mut self, dst: &KeyRange) -> bool {
        let len = self.ranges.len();
        self.ranges.retain(|r| r.equals(dst));
        len > self.ranges.len()
    }
}

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

pub(crate) const INFO_RANGE: KeyRange = KeyRange {
    left: vec![],
    right: vec![],
    inf: true,
};

impl KeyRange {
    pub fn get_range(tables: &Vec<Table>) -> KeyRange {
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

    fn equals(&self, other: &KeyRange) -> bool {
        self.left == other.left && self.right == self.right && self.inf == self.inf
    }

    fn overlaps_with(&self, other: &KeyRange) -> bool {
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
