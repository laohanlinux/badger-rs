#![feature(pointer_byte_offsets)]
#![feature(sync_unsafe_cell)]
#![feature(associated_type_defaults)]
#![feature(type_alias_impl_trait)]
#![feature(strict_provenance_atomic_ptr)]
#![feature(atomic_from_mut)]
#![feature(cursor_remaining)]
#![feature(pattern)]
#![feature(cell_leak)]
#![feature(path_file_prefix)]
#![feature(fs_try_exists)]
#![feature(generic_associated_types)]

extern crate core;

use std::io;
use std::mem::align_of;

mod event;
mod kv;
mod log_file;
mod manifest;
mod options;
mod skl;
mod table;
mod types;
mod value_log;
#[cfg(test)]
mod value_log_tests;
mod y;

#[cfg(test)]
mod test_util;
mod levels;
mod level_handler;
mod compaction;

pub use skl::{Arena, Node, SkipList};
pub use y::{Error, Result};

#[allow(dead_code)]
#[inline]
pub(crate) fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}
