#![feature(pointer_byte_offsets)]
#![feature(sync_unsafe_cell)]
#![feature(associated_type_defaults)]
#![feature(type_alias_impl_trait)]

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

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum BadgerErr {}
