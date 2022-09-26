mod iterator;
mod metrics;

pub use iterator::ValueStruct;
use thiserror::Error;

/// Constants use in serialization sizes, and in ValueStruct serialization
pub const META_SIZE: usize = 1;
pub const USER_META_SIZE: usize = 1;
pub const CAS_SIZE: usize = 8;
pub const VALUE_SIZE: usize = 4;

// Indicates an end of file when trying to read from a memory mapped file
// and encountering the end of slice.
#[derive(Error, Debug)]
pub enum BadgerError {
    #[error("end of mapped region")]
    EOF,
}
