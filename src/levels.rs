use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use awaitgroup::WaitGroup;
use crate::compaction::CompactStatus;
use crate::kv::{KV, WeakKV};
use crate::level_handler::LevelHandler;

pub(crate) struct LevelsController {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandler>>,
    kv: *const KV,
    // Atomic
    next_file_id: AtomicI64,
    // For ending compactions.
    compact_worker_wg: WaitGroup,
    c_status: CompactStatus,
}

impl Default for LevelsController {
    fn default() -> Self {
        todo!()
    }
}

struct LevelsControllerInner {
    // The following are initialized once and const
    levels: Arc<Vec<LevelHandler>>,
    kv: WeakKV,
    // Atomic
    next_file_id: AtomicI64,
    // For ending compactions.
    compact_worker_wg: WaitGroup,
    c_status: CompactStatus,
}

impl LevelsControllerInner {
    // Returns true if level zero may be compacted, without accounting for compactions that already
    // might be happening.
    fn is_level0_compact_table(&self) -> bool {
       // self.levels[0]
        todo!()
    }
}