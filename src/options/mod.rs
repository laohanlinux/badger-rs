use crate::value_log::Entry;
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE};
use crate::Node;
use rand::random;
use std::env::temp_dir;

/// Specifies how data in LSM table files and value log files should
/// be loaded.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileLoadingMode {
    /// Indicates that files must be loaded using standard I/O
    FileIO,
    /// Indicates that files must be loaded into RAM
    LoadToRADM,
    /// Indicates that the file must be memory-mapped
    MemoryMap,
}

/// Params for creating DB object.
#[derive(Debug, Clone)]
pub struct Options {
    /// 1. Mandatory flags
    /// -------------------
    /// Directory to store the data in. Should exist and be writable.
    pub dir: Box<String>,
    /// Directory to store the value log in. Can be the same as Dir. Should
    /// exist and be writable.
    pub value_dir: Box<String>,
    /// 2. Frequently modified flags
    /// -----------------------------
    /// Sync all writes to disk. Setting this to true would slow down data
    /// loading significantly.
    pub sync_writes: bool,
    /// How should LSM tree be accessed.
    pub table_loading_mode: FileLoadingMode,
    /// 3. Flags that user might want to review
    /// ----------------------------------------
    /// The following affect all levels of LSM tree.
    /// Each table (or file) is at most this size.
    pub max_table_size: u64,
    /// Equals SizeOf(Li+1)/SizeOf(Li).
    pub level_size_multiplier: u64,
    /// Maximum number of levels of compaction.
    pub max_levels: usize,
    /// If value size >= this threshold, only store value offsets in tree.
    pub value_threshold: usize,
    /// Maximum number of tables to keep in memory, before stalling.
    pub num_mem_tables: usize,
    /// The following affect how we handle LSM tree L0.
    /// Maximum number of Level 0 tables before we start compacting.
    pub num_level_zero_tables: usize,

    /// If we hit this number of Level 0 tables, we will stall until L0 is
    /// compacted away.
    pub num_level_zero_tables_stall: usize,

    /// Maximum total size for L1.
    pub level_one_size: u64,

    /// Size of single value log file.
    pub value_log_file_size: u64,

    /// Number of compaction workers to run concurrently.
    pub num_compactors: u64,

    /// 4. Flags for testing purposes
    /// ------------------------------
    /// Stops LSM tree from compactions.
    pub do_not_compact: bool,
    /// max entries in batch
    pub max_batch_count: u64,
    // max batch size in bytes
    pub max_batch_size: u64,
}

impl Options {
    pub fn estimate_size(&self, entry: &Entry) -> usize {
        if entry.value.len() < self.value_threshold {
            return entry.key.len() + entry.value.len() + META_SIZE + USER_META_SIZE + CAS_SIZE;
        }
        entry.key.len() + 16 + META_SIZE + USER_META_SIZE + CAS_SIZE
    }

    /// Return the size of allocator arena
    pub fn arena_size(&self) -> u64 {
        self.max_table_size + self.max_batch_size + self.max_batch_count * (Node::align_size() as u64)
    }
}

impl Default for Options {
    fn default() -> Self {
        let id = random::<u64>();
        Options {
            dir: Box::new(id.to_string()),
            value_dir: Box::new(id.to_string()),
            sync_writes: false,
            table_loading_mode: FileLoadingMode::LoadToRADM,
            max_table_size: 64 << 20,
            level_size_multiplier: 10,
            max_levels: 7,
            value_threshold: 20,
            num_mem_tables: 5,
            num_level_zero_tables: 5,
            num_level_zero_tables_stall: 10,
            level_one_size: 256 << 20,
            value_log_file_size: 1 << 30,
            num_compactors: 3,
            do_not_compact: false,
            max_batch_count: 200,
            max_batch_size: 1 << 13,
        }
    }
}
