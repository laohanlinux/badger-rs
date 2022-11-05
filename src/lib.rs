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

extern crate core;

use std::io;
use std::mem::align_of;

mod options;
mod skl;
mod table;
mod y;
mod value_log;
mod event;
mod types;
mod log_file;
mod kv;
mod value_log_tests;

pub use skl::{Arena, Node, SkipList};
pub use y::{Error, Result};

#[allow(dead_code)]
#[inline]
pub(crate) fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}
