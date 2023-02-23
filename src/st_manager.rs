use crate::SkipList;
use crossbeam_epoch::{Atomic, Shared};
use std::borrow::Borrow;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct SkipListManager {
    mt: Atomic<Option<SkipList>>,
    imm: Arc<RwLock<Vec<SkipList>>>,
}

impl Default for SkipListManager {
    fn default() -> Self {
        todo!()
    }
}

// impl SkipListManager {
//     pub unsafe fn must_mt(&self) -> Option<&'_ Option<SkipList>> {
//         let p = &crossbeam_epoch::pin();
//         let mt = self.mt.load(Ordering::SeqCst, p);
//         mt.as_ref()
//     }
// }

#[test]
fn ti() {}
