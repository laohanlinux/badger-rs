use crate::types::{Channel, Closer};
use crate::value_log::{Entry, Header, ValuePointer};
use crate::y::{create_synced_file, Result};
use crate::y::{is_eof, read_at, Decode};
use crate::Error;
use async_channel::Sender;
use byteorder::{BigEndian, ReadBytesExt};
use drop_cell::defer;
use either::Either;
use log::info;
use memmap::{Mmap, MmapMut};
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::async_iter::AsyncIterator;
use std::env::temp_dir;
use std::f32::consts::E;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::future::Future;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::time::SystemTime;
use tokio::select;

// MmapType is a Mmap and MmapMut tule
pub(crate) struct MmapType(Either<Mmap, MmapMut>);

impl MmapType {
    pub(crate) fn get_mmap(&self) -> &Mmap {
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

    pub(crate) fn get_mut_mmap_ref(&mut self) -> &mut MmapMut {
        match self.0 {
            Either::Right(ref mut m) => m,
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
    // async read *n* entries
    pub(crate) async fn read_entries(
        &self,
        offset: u32,
        n: usize,
    ) -> Result<(Vec<(Entry, ValuePointer)>, u32)> {
        let m = self.mmap_slice();
        let mut cursor_offset = offset;
        let mut v = vec![];
        while cursor_offset < m.len() as u32 && v.len() < n {
            let entry = Entry::from_slice(cursor_offset, m)?;
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

    pub(crate) async fn async_iterate_by_offset(
        &self,
        ctx: Closer,
        mut offset: u32,
        notify: Sender<(Entry, ValuePointer)>,
    ) {
        defer! {ctx.done()}
        defer! {notify.close();}
        let has_been_close = ctx.has_been_closed();
        loop {
            let (v, next) = self.read_entries(offset, 1).await.unwrap();
            offset = next;
            if v.is_empty() {
                return;
            } else {
                // TODO batch sender
                for item in v {
                    select! {
                       _ = has_been_close.recv() => {},
                       _ = notify.send(item) => {},
                    }
                }
            }
        }
    }

    // async iterate from offset that must be call with thread safety
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
                if !f(entry, vptr).await? {
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
        let mut truncate = false; // because maybe abort before write
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
            entry.cas_counter = AtomicU64::new(h.cas_counter);
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
    // new LogFile with special path.
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

    // open only read permission
    pub(crate) fn open_read_only(&mut self) -> Result<()> {
        let fd = std::fs::OpenOptions::new()
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

    // Acquire lock on mmap if you are calling this.
    pub(crate) fn read(&self, p: &ValuePointer) -> Result<&[u8]> {
        info!(
            "ready to read bytes from mmap, {}, {:?}",
            self._mmap.as_ref().unwrap().is_left(),
            p
        );
        let offset = p.offset;
        let mmp = self._mmap.as_ref().unwrap();
        // todo add metrics
        match mmp.0 {
            Either::Left(ref m) => Ok(&m.as_ref()[offset as usize..(offset + p.len) as usize]),
            Either::Right(ref m) => Ok(&m.as_ref()[offset as usize..(offset + p.len) as usize]),
        }
    }

    // Done written, reopen with read only permisson for file and mmap.
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

    pub(crate) fn set_write(&mut self, sz: u64) -> Result<()> {
        self.fd.as_mut().unwrap().set_len(sz as u64)?;
        info!("reset file size:{}", sz);
        let mut _mmap = unsafe { Mmap::map(&self.fd.as_ref().unwrap())?.make_mut()? };
        self._mmap.replace(MmapType(Either::Right(_mmap)));
        self.sz = sz as u32;
        Ok(())
    }

    // return mmap slice
    fn mmap_slice(&self) -> &[u8] {
        let mmap = self._mmap.as_ref().unwrap();
        match mmap.0 {
            Either::Left(ref _mmap) => _mmap.as_ref(),
            Either::Right(ref _mmap) => _mmap.as_ref(),
        }
    }

    // return file reference
    fn file_ref(&self) -> &File {
        self.fd.as_ref().unwrap()
    }

    pub(crate) fn mut_mmap(&mut self) -> &mut MmapMut {
        let mp = self._mmap.as_mut().unwrap();
        mp.get_mut_mmap_ref()
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
    use crate::test_util;
    test_util::tracing_log();

    let tmp_path = temp_dir().join("mmap_test.txt");
    let tmp_path = tmp_path.to_str().unwrap();
    std::fs::write(tmp_path, b"hellow, word").unwrap();
    info!("path: {}", tmp_path);
    let mut vlog = LogFile::new(tmp_path).unwrap();
    vlog.fd.take();
    vlog.fd = Some(create_synced_file(tmp_path, true).unwrap());
    info!(
        "{},{:?}",
        vlog.sz,
        String::from_utf8_lossy(vlog.mmap_slice())
    );
    vlog.set_write(1024).unwrap();
    // vlog.fd.as_mut().unwrap().write_all(b"foobat").unwrap();
    // vlog.fd.as_mut().unwrap().sync_all().unwrap();
    // vlog.mut_mmap().flush_async().unwrap();
    {
        let mut buffer = vlog._mmap.as_mut().unwrap();
        let mut buffer = buffer.get_mut_mmap_ref();
        let mut wt = buffer.as_mut();
        wt.write_all(b"1234").unwrap();
    }
    info!(
        "{},{:?}",
        vlog.sz,
        String::from_utf8_lossy(vlog.mmap_slice())
    );
}
