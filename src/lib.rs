#![feature(pointer_byte_offsets)]
#![feature(sync_unsafe_cell)]
#[allow(invalid_type_param_default)]

use std::mem::align_of;

extern crate core;

mod options;
mod skl;
mod y;

#[allow(dead_code)]
#[inline]
pub(crate) fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}
