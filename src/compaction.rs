use crate::hex_str;
use crate::levels::CompactDef;
use crate::table::table::Table;

use log::{error, info, warn};
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct CompactStatus {
    // every level has a *CompactionStatus* that includes multipart *KeyRange*
    pub(crate) levels: RwLock<Vec<LevelCompactStatus>>,
}

impl Default for CompactStatus {
    fn default() -> Self {
        CompactStatus {
            levels: RwLock::new(vec![]),
        }
    }
}

impl CompactStatus {
    // Check whether we can run this *CompactDef*. That it doesn't overlap with any
    // other running Compaction. If it can be run, it would store this run in the compactStatus state.
    pub(crate) fn compare_and_add(&self, cd: &CompactDef) -> bool {
        let level = cd.this_level.level();
        assert!(
            level + 1 < self.rl().len(),
            "Got level {}, max level {}",
            level,
            self.rl().len()
        );
        let lc = self.rl();
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
            log::info!(
                "skip the compaction, top_size:{}, bot_size:{}, max_size:{}",
                cd.this_level.get_total_size(),
                cd.next_level.get_total_size(),
                cd.this_level.get_max_total_size()
            );
            return false;
        }
        this_level.add(cd.this_range.clone());
        next_level.add(cd.next_range.clone());
        this_level.incr_del_size(cd.this_size.load(Ordering::Relaxed));
        true
    }

    // Delete CompactDef.
    pub(crate) fn delete(&self, cd: &CompactDef) {
        let levels = self.wl();
        let level = cd.this_level.level();
        assert!(
            level < levels.len() - 1,
            "Got level {}, Max levels {}",
            level,
            levels.len()
        );

        let this_level = levels.get(level).unwrap();
        let next_level = levels.get(level + 1).unwrap();
        // Decr delete size after compacted.
        this_level.decr_del_size(cd.this_size.load(Ordering::Relaxed));
        let mut found = this_level.remove(&cd.this_range);
        // top level must have KeyRange because it is compact's base condition
        assert!(found, "{}", this_level);
        found = next_level.remove(&cd.next_range) && found;
        if !found {
            let this_kr = &cd.this_range;
            let next_kr = &cd.next_range;
            warn!("Looking for: [{}] in this level.", this_kr,);
            warn!("This Level: {}", level);
            warn!("Looking for: [{}] in next level.", next_kr);
            warn!("Next Level: {}", level + 1);
            warn!("KeyRange not found");
            warn!("Looking for seek k range");
            warn!("{}, {}", cd.this_range, cd.next_range);
        }
    }

    // Return trur if the level overlap with this, otherwise false
    pub(crate) fn overlaps_with(&self, level: usize, this: &KeyRange) -> bool {
        self.rl()[level].overlaps_with(this)
    }

    // Return level's deleted data count
    pub(crate) fn del_size(&self, level: usize) -> u64 {
        self.rl()[level].get_del_size()
    }

    // Return Level's compaction status with *WriteLockGuard*
    pub(crate) fn wl(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<LevelCompactStatus>> {
        self.levels.write()
    }

    // Return Level's compaction status with *ReadLockGuard*
    pub(crate) fn rl(&self) -> RwLockReadGuard<'_, RawRwLock, Vec<LevelCompactStatus>> {
        self.levels.read()
    }

    pub(crate) fn to_log(&self) {
        let status = self.rl();
        info!("Compact levels, count:{}", status.len());
        for level in status.iter().enumerate() {
            info!("[{}] {}", level.0, level.1.to_string())
        }
    }
}

// Every level compacted status(ranges).
#[derive(Clone, Debug)]
pub(crate) struct LevelCompactStatus {
    ranges: Arc<RwLock<Vec<KeyRange>>>,
    del_size: Arc<AtomicU64>, // all KeyRange size at the level (NOTE: equal LevelCompactStatus.ranges delete size)
}

impl Default for LevelCompactStatus {
    fn default() -> Self {
        LevelCompactStatus {
            ranges: Arc::new(RwLock::new(Vec::new())),
            del_size: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Display for LevelCompactStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let ranges = self
            .rl()
            .iter()
            .map(|kr| kr.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let del_size = self.get_del_size();
        f.debug_struct("LevelCompactStatus")
            .field("ranges", &ranges)
            .field("del_size", &del_size)
            .finish()
    }
}

impl LevelCompactStatus {
    // returns true if self.ranges and dst has overlap, otherwise returns false
    fn overlaps_with(&self, dst: &KeyRange) -> bool {
        #[cfg(test)]
        log::info!(
            "level compact status compare, {:?}, dst: {:?}",
            self.rl(),
            dst
        );
        self.rl().iter().any(|kr| kr.overlaps_with(dst))
    }

    // remove dst from self.ranges
    pub(crate) fn remove(&self, dst: &KeyRange) -> bool {
        let mut rlock = self.wl();
        let len = rlock.len();
        //  rlock.retain(|r| r == dst);
        rlock.retain(|r| r != dst);
        len > rlock.len()
    }

    // add dst range
    fn add(&self, dst: KeyRange) {
        self.wl().push(dst);
    }

    pub(crate) fn get_del_size(&self) -> u64 {
        self.del_size.load(Ordering::Relaxed)
    }

    fn incr_del_size(&self, n: u64) {
        self.del_size.fetch_add(n, Ordering::Relaxed);
    }

    fn decr_del_size(&self, n: u64) {
        self.del_size.fetch_sub(n, Ordering::Relaxed);
    }

    fn wl(&self) -> RwLockWriteGuard<'_, RawRwLock, Vec<KeyRange>> {
        self.ranges.write()
    }

    fn rl(&self) -> RwLockReadGuard<'_, RawRwLock, Vec<KeyRange>> {
        self.ranges.read()
    }
}

// [left, right], Special inf is range all if it be set `true`
#[derive(Clone, Default, Debug)]
pub(crate) struct KeyRange {
    pub(crate) left: Vec<u8>,
    // TODO zero Copy
    pub(crate) right: Vec<u8>,
    pub(crate) inf: bool,
}

impl PartialEq for KeyRange {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl Display for KeyRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<left={}, right={}, inf={}>",
            hex_str(&self.left),
            hex_str(&self.right),
            self.inf
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

        // ---[other_left, other_right]--[]
        if self.left > other.right {
            return false;
        }
        // ---[]--[other-left, other-right]
        if self.right < other.left {
            return false;
        }
        true
    }
}

mod tests {
    use crate::compaction::{KeyRange, INFO_RANGE};

    #[test]
    fn key_range() {
        let mut v = vec![KeyRange {
            left: vec![],
            right: vec![],
            inf: true,
        }];
        let cd = INFO_RANGE;
        v.retain(|kr| kr != &cd);
        println!("{:?}, {}", v, cd);

        let tests = vec![vec![2, 20], vec![30, 50], vec![70, 80]];

        let inputs = vec![
            vec![0, 1],
            vec![81, 100],
            vec![21, 25],
            vec![29, 40],
            vec![40, 60],
            vec![21, 51],
            vec![21, 100],
            vec![0, 200],
            vec![0, 70],
            vec![70, 80],
        ];

        for (i, arg) in inputs.iter().enumerate() {
            let left = tests.binary_search_by(|probe| probe[1].cmp(&arg[0]));
            let left = left.unwrap_or_else(|n| n);
            let right = tests.binary_search_by(|probe| probe[0].cmp(&arg[1]));
            let right = right.map(|n| n + 1).unwrap_or_else(|n| n);
            println!("{}, {:?}, {:?}", i, left, right);
        }
    }
}
