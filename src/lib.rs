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
#![feature(unwrap_infallible)]
#![feature(slice_pattern)]
#![feature(slice_take)]
#![feature(arc_into_inner)]
#![feature(async_closure)]
#![feature(async_iterator)]
#![feature(atomic_mut_ptr)]

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

mod compaction;
mod level_handler;
mod levels;
mod pb;
#[cfg(test)]
mod test_util;
mod st_manager;

pub use skl::*;
pub use y::*;
pub use st_manager::*;

#[allow(dead_code)]
#[inline]
pub(crate) fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}
