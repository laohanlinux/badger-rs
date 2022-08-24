use std::cell::SyncUnsafeCell;
use std::mem::ManuallyDrop;
use std::thread::spawn;

#[derive(Clone)]
pub struct SafeVec {
    v: ManuallyDrop<Vec<u8>>,
}
