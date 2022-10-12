pub(crate) mod iterator;
mod metrics;

use crate::Error::Unexpected;
pub use iterator::ValueStruct;
use memmap::{Mmap, MmapMut};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::Hasher;
use std::io;
use std::io::ErrorKind;
use thiserror::Error;

/// Constants use in serialization sizes, and in ValueStruct serialization
pub const META_SIZE: usize = 1;
pub const USER_META_SIZE: usize = 1;
pub const CAS_SIZE: usize = 8;
pub const VALUE_SIZE: usize = 4;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("io error: {0}")]
    Io(String),
    #[error("{0}")]
    Unexpected(String),

    /// Return when a log file containing the value is not found.
    /// This usually indicates that it may have been garbage collected, and the
    /// operation needs to be retried.
    #[error("Unable to find log file. Please retry")]
    ValueRetry,
    /// Returned when a CompareAndSet operation has failed due
    /// to a counter mismatch.
    #[error("CompareAndSet failed due to counter mismatch")]
    ValueCasMisMatch,
    /// Returned
    #[error("SetIfAbsent failed since key already exists")]
    ValueKeyExists,
    /// Returned if threshold is set to zero, and value log GC is called.
    /// In such a case, GC can't be run.
    #[error("Value log GC can't run because threshold is set to zero")]
    ValueThresholdZero,
    /// Returned if a call for value log GC doesn't result in a log file rewrite.
    #[error("Value log GC attempt didn't result in any cleanup")]
    ValueNoRewrite,
    /// Returned if a value log GC is called either while another GC is running, or
    /// after KV::Close has been called.
    #[error("Value log GC request rejected")]
    ValueRejected,
    /// Returned if the user request is invalid.
    #[error("Invalid request")]
    ValueInvalidRequest,
}

impl Default for Error {
    fn default() -> Self {
        Self::Unexpected("".into())
    }
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Self {
        Self::Io(e.kind().to_string())
    }
}

impl From<&'static str> for Error {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self::Unexpected(s.to_string())
    }
}

impl From<String> for Error {
    #[inline]
    fn from(s: String) -> Self {
        Self::Unexpected(s)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn is_eof<T>(ret: &io::Result<T>) -> bool {
    if ret.is_ok() {
        return false;
    }
    match ret {
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => true,
        _ => false,
    }
}

pub fn hash(buffer: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::default();
    hasher.write(buffer);
    hasher.finish()
}

pub fn mmap(fd: &File, writable: bool, size: usize) -> Result<MmapMut> {
    let m = unsafe {
        memmap::MmapOptions::new()
            .offset(0)
            .len(size)
            .map_mut(fd)
            .map_err(|_| "Failed to mmap")?
    };
    Ok(m)
}

pub fn open_synced_file(file_name: &str, sync: bool) -> Result<File> {
    let file = File::options()
        .write(true)
        .read(true)
        .create(true)
        .append(true)
        .open(file_name)
        .or_else(Err)?;
    Ok(file)
}
