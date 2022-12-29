use crate::table::table::TableCore;
use parking_lot::Mutex;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub(crate) struct CompactStatus {
    levels: Arc<Mutex<Vec<LevelCompactStatus>>>,
}

impl CompactStatus {
    fn to_log(&self) {
        todo!()
    }

    // fn compare_and_add(&self, cd:)

    fn overlaps_with(&self, level: usize, this: &KeyRange) -> bool {
        let compact_status = self.levels.lock();
        compact_status[level].overlaps_with(this)
    }

    fn del_size(&self, level: usize) -> i64 {
        let compact_status = self.levels.lock();
        compact_status[level].del_size
    }
}

#[derive(Debug)]
pub(crate) struct LevelCompactStatus {
    ranges: Vec<KeyRange>,
    del_size: i64,
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
    left: Vec<u8>, // TODO zero Copy
    right: Vec<u8>,
    inf: bool,
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

const INFO_RANGE: KeyRange = KeyRange {
    left: vec![],
    right: vec![],
    inf: false,
};

impl KeyRange {
    fn get_range(tables: &Vec<TableCore>) -> KeyRange {
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
