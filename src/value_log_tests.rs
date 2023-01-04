use crate::kv::KV;
use crate::options::Options;
use crate::test_util::{mock_log, mock_log_terminal, random_tmp_dir};
use crate::value_log::{Entry, MetaBit, Request};
use std::cell::RefCell;
use std::env::temp_dir;
use std::fs;
use std::sync::Arc;
use awaitgroup::WaitGroup;

fn new_test_options(dir: String) -> Options {
    let mut opt = Options::default();
    opt.max_table_size = 1 << 15; // Force more compaction
    opt.level_one_size = 4 << 15; // Force more compaction.
    opt.dir = Box::new(dir.clone());
    opt.value_dir = Box::new(dir.clone());
    return opt;
}

// #[test]
// fn value_basic() {
//     mock_log_terminal();
//     let dir = random_tmp_dir();
//     println!("{}", dir);
//     let mut kv = KV::new(new_test_options(dir)).unwrap();
//     // Use value big enough that the value log writes them even if SyncWrites is false.
//     let val1 = b"sampleval012345678901234567890123";
//     let val2 = b"samplevalb012345678901234567890123";
//     assert!(val1.len() >= kv.opt.value_threshold);
//
//     let entry1 = Entry {
//         key: b"samplekey".to_vec(),
//         value: val1.to_vec(),
//         meta: MetaBit::BitValuePointer.bits(),
//         cas_counter_check: 22222,
//         cas_counter: 33333,
//         offset: 0,
//         user_meta: 0,
//     };
//     let entry2 = Entry {
//         key: b"samplekeyb".to_vec(),
//         value: val2.to_vec(),
//         meta: MetaBit::BitValuePointer.bits(),
//         cas_counter_check: 22225,
//         cas_counter: 33335,
//         offset: 0,
//         user_meta: 0,
//     };
//
//     let mut wait = WaitGroup::new();
//     let b = Request {
//         entries: vec![RefCell::new(entry1), RefCell::new(entry2)],
//         ptrs: RefCell::new(vec![]),
//         wait_group: RefCell::new(Some(wait.worker())),
//         err: RefCell::new(Arc::new(Ok(()))),
//     };
//     // todo add event stats
//
//     kv.must_mut_vlog()
//         .write(&vec![b])
//         .expect("TODO: panic message");
// }
