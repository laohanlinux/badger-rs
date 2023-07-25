mod codec;
pub(crate) mod iterator;
pub mod merge_iterator;
mod metrics;

pub use codec::{AsyncEncDec, Decode, Encode};
pub use iterator::*;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use libc::O_DSYNC;
use log::error;
use memmap::MmapMut;
pub use merge_iterator::*;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{ErrorKind, Write};

use std::{array, cmp, io};
use std::backtrace::Backtrace;
use thiserror::Error;
use tracing::info;

#[cfg(any(target_os = "windows"))]
use winapi::um::winbase;

pub const EMPTY_SLICE: Vec<u8> = vec![];

/// Constants use in serialization sizes, and in ValueStruct serialization
pub const META_SIZE: usize = 1;
pub const USER_META_SIZE: usize = 1;
pub const CAS_SIZE: usize = 8;
pub const VALUE_SIZE: usize = 4;

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error(transparent)]
    StdIO(#[from] eieio::Error),

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
    #[error("Invalid Dir, directory does not exist")]
    InValidDir,
    #[error("Invalid ValueLogFileSize, must be between 1MB and 2GB")]
    ValueLogSize,

    //////////////////////////////////
    // valueLog error
    /////////////
    #[error("Too few bytes read")]
    TooFewBytes,
    /// Indicates an end of file then trying to read from a memory mapped file
    /// and encountering the end of slice.
    #[error("End of mapped region")]
    EOF,
    #[error("Manifest has bad magic")]
    BadMagic,
    /////////////////////////////////
    #[error("Not found")]
    NotFound,
    ////////////////////////////////
    // GC
    #[error("Stop iteration")]
    StopGC,
}

impl Default for Error {
    fn default() -> Self {
        Self::Unexpected("".into())
    }
}

impl Error {
    pub fn is_io(&self) -> bool {
        match self {
            Error::StdIO(_err) => true,
            _ => false,
        }
    }

    pub fn is_io_eof(&self) -> bool {
        match self {
            Error::StdIO(err) if err.kind() == ErrorKind::UnexpectedEof => true,
            _ => false,
        }
    }

    pub fn is_io_existing(&self) -> bool {
        match self {
            Error::StdIO(err) => {
                if err.kind() == io::ErrorKind::AlreadyExists {
                    return true;
                }
                if let Some(code) = err.raw_os_error() {
                    return code == 2;
                }
                false
            }
            _ => false,
        }
    }

    pub fn is_io_notfound(&self) -> bool {
        match self {
            Error::StdIO(err) if err.kind() == ErrorKind::NotFound => true,
            _ => false,
        }
    }

    pub fn is_not_found(&self) -> bool {
        match self {
            Error::NotFound => true,
            _ => false,
        }
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

impl From<std::io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::StdIO(eieio::Error::from(value))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[inline]
pub fn is_eof<T>(ret: &io::Result<T>) -> bool {
    if ret.is_ok() {
        return false;
    }
    match ret {
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => true,
        _ => false,
    }
}

#[inline]
pub fn is_existing<T>(ret: &io::Result<T>) -> bool {
    if ret.is_ok() {
        return false;
    }
    match ret {
        Err(err) if err.kind() == ErrorKind::AlreadyExists => true,
        _ => false,
    }
}

pub fn hash(buffer: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::default();
    hasher.write(buffer);
    hasher.finish()
}

pub fn mmap(fd: &File, _writable: bool, size: usize) -> Result<MmapMut> {
    let m = unsafe {
        memmap::MmapOptions::new()
            .offset(0)
            .len(size)
            .map_mut(fd)
            .map_err(|_| "Failed to mmap")?
    };
    Ok(m)
}

pub fn open_synced_file(file_name: &str, _sync: bool) -> Result<File> {
    let file = File::options()
        .write(true)
        .read(true)
        .create(true)
        .append(true)
        .open(file_name)
        .or_else(Err)?;
    Ok(file)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(crate) fn read_at(fp: &File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::unix::fs::FileExt;
    fp.read_at(buffer, offset).map_err(|err| err.into())
}

#[cfg(target_os = "windows")]
pub(crate) fn read_at(fp: &File, buffer: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::windows::fs::FileExt;
    fp.seek_read(buffer, offset).map_err(|err| err.into())
}

pub(crate) fn num_cpu() -> usize {
    let n = num_cpus::get();
    n
}

// todo add error
pub(crate) fn parallel_load_block_key(fp: File, offsets: Vec<u64>) -> Vec<Vec<u8>> {
    use crate::table::builder::Header;
    use std::sync::mpsc::sync_channel;
    use threads_pool::*;
    let (tx, rx) = sync_channel(offsets.len());
    let num = num_cpu();
    let mut pool = ThreadPool::new(num);
    for (i, offset) in offsets.iter().enumerate() {
        let offset = *offset;
        let fp = fp.try_clone().unwrap();
        let tx = tx.clone();
        pool.execute(move || {
            let mut buffer = vec![0u8; Header::size()];
            read_at(&fp, &mut buffer, offset).unwrap();
            let head = Header::from(buffer.as_slice());
            assert_eq!(
                head.p_len, 0,
                "key offset: {}, h.p_len = {}",
                offset, head.p_len
            );
            let out = vec![0u8; head.k_len as usize];
            read_at(&fp, &mut buffer, offset + Header::size() as u64).unwrap();
            tx.send((i, out)).unwrap();
        })
            .unwrap();
    }
    pool.close();

    let mut keys = vec![vec![0u8]; offsets.len()];
    for _ in 0..offsets.len() {
        let (i, key) = rx.recv().unwrap();
        keys[i] = key;
    }
    drop(tx);
    keys
}

pub(crate) fn slice_cmp_gte(a: &[u8], b: &[u8]) -> cmp::Ordering {
    match a.cmp(&b) {
        cmp::Ordering::Less => cmp::Ordering::Less,
        cmp::Ordering::Greater => cmp::Ordering::Equal,
        cmp::Ordering::Equal => cmp::Ordering::Equal,
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
pub(crate) fn open_existing_synced_file(file_name: &str, synced: bool) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    if synced {
        File::options()
            .write(true)
            .read(true)
            .custom_flags(O_DSYNC)
            .open(file_name)
            .map_err(|err| err.into())
    } else {
        File::options()
            .write(true)
            .read(true)
            .open(file_name)
            .map_err(|err| err.into())
    }
}

#[cfg(any(target_os = "windows"))]
pub(crate) fn open_existing_synced_file(file_name: &str, synced: bool) -> Result<File> {
    use std::fs::OpenOptions;
    use std::os::windows::prelude::*;
    use winapi::um::winbase;
    if synced {
        File::options()
            .write(true)
            .read(true)
            // .custom_flags(winbase::FILE_FLAG_WRITE_THROUGH)
            .open(file_name)
            .map_err(|err| err.into())
    } else {
        File::options()
            .write(true)
            .read(true)
            .open(file_name)
            .map_err(|err| err.into())
    }
}

pub(crate) fn create_synced_file(file_name: &str, _synce: bool) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .append(true)
        .open(file_name)
        .map_err(|err| err.into())
}

pub(crate) fn async_create_synced_file(file_name: &str, synced: bool) -> Result<tokio::fs::File> {
    let fp = create_synced_file(file_name, synced)?;
    Ok(tokio::fs::File::from_std(fp))
}

pub(crate) fn sync_directory(d: &str) -> Result<()> {
    let fp = File::open(d)?;
    fp.sync_all().map_err(|err| err.into())
}

pub(crate) async fn async_sync_directory(d: String) -> Result<()> {
    let fp = tokio::fs::File::open(d).await?;
    fp.sync_all().await?;
    Ok(())
}

pub(crate) fn hex_str(buf: &[u8]) -> String {
    String::from_utf8(buf.to_vec()).unwrap_or_else(|_| "Sorry, Hex String Failed!!!".to_string())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[test]
fn dsync() {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options.write(true);

    options.custom_flags(libc::O_WRONLY);
    let file = options.open("foo.txt");
    println!("{:?}", file.err());
}

// find a value in array with binary search
pub fn binary_search<T: Ord, F>(array: &[T], f: F) -> Option<usize>
    where
        F: Fn(&T) -> Ordering,
{
    let mut low = 0;
    let mut high = array.len() - 1;
    while low <= high {
        let mid = (low + high) / 2;
        match f(&array[mid]) {
            Ordering::Equal => return Some(mid),
            Ordering::Less => {
                low = mid + 1;
            }
            Ordering::Greater => {
                if mid <= 0 {
                    break;
                }
                high = mid - 1;
            }
        }
    }

    None
}

#[test]
fn print_backtrace() {
    let buffer = Backtrace::force_capture();
    let mut frames = buffer.frames();
    if frames.len() > 5 {
        frames = &frames[0..5];
    }
    for frame in frames {
        info!("{:?}", frame)
    }
}

#[test]
fn binary_search_test() {
    let v = &[1, 2, 3, 4, 5];
    for t in v {
        let ok = binary_search(v, |v| v.cmp(t)).unwrap();
        assert!(v[ok].eq(t));
    }
    for t in &[0, 6, 7] {
        let ok = binary_search(v, |v| v.cmp(t));
        assert!(ok.is_none());
    }
}
