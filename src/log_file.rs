use crate::value_log::{Entry, Header, ValuePointer};
use crate::y::{create_synced_file, Result};
use crate::y::{is_eof, read_at, Decode};
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt};
use core::slice::SlicePattern;
use either::Either;
use memmap::{Mmap, MmapMut};
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::async_iter::AsyncIterator;
use std::env::temp_dir;
use std::f32::consts::E;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

pub(crate) struct MmapType(Either<Mmap, MmapMut>);

impl MmapType {
    fn get_mmap(&self) -> &Mmap {
        match self.0 {
            Either::Left(ref _mmap) => _mmap,
            _ => panic!("It should be not happen"),
        }
    }

    pub(crate) fn get_mut_mmap(&self) -> &MmapMut {
        match self.0 {
            Either::Right(ref m) => m,
            _ => panic!("It should be not happen"),
        }
    }
}

impl Deref for MmapType {
    type Target = Either<Mmap, MmapMut>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Mmap> for MmapType {
    fn from(value: Mmap) -> Self {
        Self(Either::Left(value))
    }
}

impl From<MmapMut> for MmapType {
    fn from(value: MmapMut) -> Self {
        Self(Either::Right(value))
    }
}

pub(crate) struct LogFile {
    pub(crate) _path: Box<String>,
    pub(crate) fd: Option<File>,
    pub(crate) fid: u32,
    pub(crate) _mmap: Option<MmapType>,
    pub(crate) sz: u32,
}

impl Debug for LogFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFile")
            .field("path", self._path.as_ref())
            .field("fd", &self.fid)
            .field("size", &self.sz)
            .finish()
    }
}

impl LogFile {
    pub(crate) async fn read_entries(
        &self,
        offset: u32,
        n: usize,
    ) -> Result<(Vec<(Entry, ValuePointer)>, u32)> {
        let m = self.mmap_slice();
        let mut cursor_offset = offset;
        let mut v = vec![];
        while cursor_offset < m.len() as u32 && v.len() < n {
            let mut entry = Entry::from_slice(cursor_offset, m)?;
            let mut vpt = ValuePointer::default();
            vpt.fid = self.fid;
            vpt.len =
                Header::encoded_size() as u32 + (entry.key.len() + entry.value.len()) as u32 + 4;
            vpt.offset = cursor_offset;
            cursor_offset += vpt.len;
            v.push((entry, vpt))
        }
        Ok((v, cursor_offset))
    }

    pub(crate) async fn iterate_by_offset(
        &self,
        mut offset: u32,
        f: &mut impl for<'a> FnMut(
            &'a Entry,
            &'a ValuePointer,
        ) -> Pin<Box<dyn Future<Output = Result<bool>> + 'a>>,
    ) -> Result<()> {
        loop {
            let (v, next) = self.read_entries(offset, 1).await?;
            if v.is_empty() {
                return Ok(());
            }

            for (entry, vptr) in v.iter() {
                let continue_ = f(entry, vptr).await?;
                if !continue_ {
                    return Ok(());
                }
                offset = next;
            }
        }
    }

    // It should be call by one thread.
    pub(crate) async fn iterate(
        &mut self,
        offset: u32,
        f: &mut impl for<'a> FnMut(
            &'a Entry,
            &'a ValuePointer,
        ) -> Pin<Box<dyn Future<Output = Result<bool>> + 'a>>,
    ) -> Result<()> {
        let mut fd = self.fd.as_mut().unwrap();
        fd.seek(SeekFrom::Start(offset as u64))?;
        let mut entry = Entry::default();
        let mut truncate = false;
        let mut record_offset = offset;
        loop {
            let mut h = Header::default();
            let ok = h.dec(&mut fd);
            if ok.is_err() && ok.as_ref().unwrap_err().is_io_eof() {
                break;
            }
            // todo add truncate currenct
            ok?;
            if h.k_len as usize > entry.key.capacity() {
                entry.key = vec![0u8; h.k_len as usize];
            }
            if h.v_len as usize > entry.value.capacity() {
                entry.value = vec![0u8; h.v_len as usize];
            }
            entry.key.clear();
            entry.value.clear();

            let ok = fd.read(&mut entry.key);
            if is_eof(&ok) {
                break;
            }
            ok?;

            let ok = fd.read(&mut entry.value);
            if is_eof(&ok) {
                break;
            }
            ok?;
            entry.offset = record_offset;
            entry.meta = h.meta;
            entry.user_meta = h.user_mata;
            entry.cas_counter = h.cas_counter;
            entry.cas_counter_check = h.cas_counter_check;
            let ok = fd.read_u32::<BigEndian>();
            if is_eof(&ok) {
                break;
            }
            let crc = ok?;

            let mut vp = ValuePointer::default();
            vp.len = Header::encoded_size() as u32 + h.k_len + h.v_len + 4;
            record_offset += vp.len;

            vp.offset = entry.offset;
            vp.fid = self.fid;

            let _continue = f(&entry, &vp).await?;
            if !_continue {
                break;
            }
        }

        // todo add truncate
        Ok(())
    }
}

impl LogFile {
    pub(crate) fn new(path: &str) -> Result<Self> {
        let mut lf = LogFile {
            _path: Box::new(path.to_string()),
            fd: None,
            fid: 0,
            _mmap: None,
            sz: 0,
        };
        lf.open_read_only()?;
        Ok(lf)
    }

    pub(crate) fn open_read_only(&mut self) -> Result<()> {
        let mut fd = std::fs::OpenOptions::new()
            .read(true)
            .open(self._path.as_ref())?;
        let meta = fd.metadata()?;
        let file_sz = meta.len();
        let mut _mmap = unsafe { Mmap::map(&fd)? };
        self._mmap.replace(_mmap.into());
        self.fd.replace(fd);
        self.sz = file_sz as u32;
        Ok(())
    }

    fn mmap_slice(&self) -> &[u8] {
        let mmap = self._mmap.as_ref().unwrap();
        match mmap.0 {
            Either::Left(ref _mmap) => _mmap.as_ref(),
            Either::Right(ref _mmap) => _mmap.as_ref(),
        }
    }

    fn file_ref(&self) -> &File {
        self.fd.as_ref().unwrap()
    }

    // Acquire lock on mmap if you are calling this.
    pub(crate) fn read(&self, p: &ValuePointer) -> Result<&[u8]> {
        let offset = p.offset;
        let sz = self._mmap.as_ref().unwrap().len();
        let value_sz = p.len;
        return if offset >= sz as u32 || offset + value_sz > sz as u32 {
            Err(Error::EOF)
        } else {
            Ok(&self._mmap.as_ref().unwrap()[offset as usize..(offset + value_sz) as usize])
        };
        // todo add metrics
    }

    // todo opz
    pub(crate) fn done_writing(&mut self, offset: u32) -> Result<()> {
        self.sync()?;
        let mut_mmap = self.mut_mmap();
        mut_mmap.flush_async()?;
        self.fd.as_mut().unwrap().set_len(offset as u64)?;
        self.fd.as_mut().unwrap().sync_all()?;
        {
            self._mmap.take();
            self.fd.take();
        }
        self.open_read_only()
    }

    fn mut_mmap(&self) -> &MmapMut {
        self._mmap.as_ref().unwrap().get_mut_mmap()
    }

    fn mmap_ref(&self) -> &Mmap {
        self._mmap.as_ref().unwrap().get_mmap()
    }

    // You must hold lf.lock to sync()
    fn sync(&mut self) -> Result<()> {
        self.fd.as_mut().unwrap().sync_all()?;
        Ok(())
    }
}

#[test]
fn concurrency() {
    let mut lf = LogFile::new("src/test_data/vlog_file.text");
    assert!(lf.is_ok(), "{:?}", lf.unwrap_err().to_string());
}

#[test]
fn test_mmap() {
    let mut fd = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("src/test_data/vlog_file.text")
        .unwrap();

    let _mmap = unsafe { Mmap::map(&fd).unwrap() };
    println!("{}", _mmap.len());
    println!("{}", _mmap.make_mut().is_err());
}

#[test]
fn test_write_file() {
    let _path = temp_dir().join("badger-".to_owned() + &*rand::random::<u32>().to_string());
    let lf = create_synced_file(_path.to_str().unwrap(), true).unwrap();
}
