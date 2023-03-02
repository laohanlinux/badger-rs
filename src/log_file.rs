use crate::value_log::{Entry, Header, ValuePointer};
use crate::y::Result;
use crate::y::{is_eof, read_at, Decode};
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt};
use core::slice::SlicePattern;
use memmap::MmapMut;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::async_iter::AsyncIterator;
use std::fs::File;
use std::future::Future;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
// use crate::mmap::AsyncMMAP;

#[derive(Debug)]
pub(crate) struct LogFile {
    pub(crate) _path: Box<String>,
    pub(crate) fd: Option<File>,
    pub(crate) fid: u32,
    pub(crate) _mmap: Option<MmapMut>,
    pub(crate) sz: u32,
}

impl LogFile {
    // pub(crate) async fn iterate_by_offset1(
    //     _self: &Self,
    //     mut offset: u32,
    //     f: &mut impl for<'a> FnMut(
    //         &'a Entry,
    //         &'a ValuePointer,
    //     ) -> Pin<Box<dyn Future<Output = Result<bool>> + 'a>>,
    // ) -> Result<()> {
    //
    //     Ok(())
    // }

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

    pub(crate) async fn read_entries(
        &self,
        offset: u32,
        n: usize,
    ) -> Result<(Vec<(Entry, ValuePointer)>, u32)> {
        let m = self._mmap.as_ref().unwrap().as_slice();
        let mut cursor_offset = offset;
        let mut v = vec![];
        while cursor_offset < m.len() as u32 && v.len() < n {
            let mut entry = Entry::default();
            let mut h = Header::default();
            h.dec(&mut Cursor::new(
                &m[cursor_offset as usize..cursor_offset as usize + Header::encoded_size()],
            ))?;
            entry.key = vec![0u8; h.k_len as usize];
            entry.value = vec![0u8; h.v_len as usize];
            entry.meta = h.meta;
            entry.offset = cursor_offset as u32;
            entry.cas_counter = h.cas_counter;
            entry.user_meta = h.user_mata;
            entry.cas_counter_check = h.cas_counter_check;
            let mut start = cursor_offset as usize + Header::encoded_size();
            entry
                .key
                .extend_from_slice(&m[start..start + h.k_len as usize]);
            start += h.k_len as usize;
            entry
                .value
                .extend_from_slice(&m[start..start + h.v_len as usize]);
            start += h.v_len as usize;
            let crc32 = Cursor::new(&m[start..start + 4]).read_u32::<BigEndian>()?;
            let mut vpt = ValuePointer::default();
            vpt.fid = self.fid;
            vpt.len = Header::encoded_size() as u32 + h.k_len + h.v_len + 4;
            vpt.offset = cursor_offset;
            cursor_offset += vpt.len;
            v.push((entry, vpt))
        }
        Ok((v, cursor_offset))
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
        println!("file sz {}", file_sz);
        let mut _mmap = unsafe { MmapMut::map_mut(&fd)? };
        // let mut _mmap = _mmap.make_read_only()?;
        self._mmap.replace(_mmap);
        self.fd.replace(fd);
        self.sz = file_sz as u32;
        Ok(())
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
        self._mmap.as_mut().unwrap().flush_async()?;
        self.fd.as_mut().unwrap().set_len(offset as u64)?;
        self.fd.as_mut().unwrap().sync_all()?;
        {
            self._mmap.take();
            self.fd.take();
        }
        self.open_read_only()
    }

    // You must hold lf.lock to sync()
    fn sync(&mut self) -> Result<()> {
        self.fd.as_mut().unwrap().sync_all()?;
        Ok(())
    }

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

    // pub(crate) async fn iterate2(
    //     &self,
    //     offset: u32,
    //     f: &mut impl for<'a> FnMut(
    //         &'a Entry,
    //         &'a ValuePointer,
    //     ) -> Pin<Box<dyn Future<Output = Result<bool>> + 'a>>,
    // ) -> Result<()> {
    //     let mut fd = self.fd.as_mut().unwrap();
    //     fd.seek(SeekFrom::Start(offset as u64))?;
    //     // let fd = self.fd.as_ref().unwrap();
    //     let mut entry = Entry::default();
    //     let mut truncate = false;
    //     let mut record_offset = offset;
    //     loop {
    //         let mut h = Header::default();
    //         let buffer = vec![0u8; Header::encoded_size()];
    //         let ok = h.dec(&mut Cursor::new(buffer));
    //         if ok.is_err() && ok.as_ref().unwrap_err().is_io_eof() {
    //             break;
    //         }
    //         // todo add truncate currenct
    //         ok?;
    //         if h.k_len as usize > entry.key.capacity() {
    //             entry.key = vec![0u8; h.k_len as usize];
    //         }
    //         if h.v_len as usize > entry.value.capacity() {
    //             entry.value = vec![0u8; h.v_len as usize];
    //         }
    //         entry.key.clear();
    //         entry.value.clear();
    //
    //         let ok = fd.read(&mut entry.key);
    //         if is_eof(&ok) {
    //             break;
    //         }
    //         ok?;
    //
    //         let ok = fd.read(&mut entry.value);
    //         if is_eof(&ok) {
    //             break;
    //         }
    //         ok?;
    //         entry.offset = record_offset;
    //         entry.meta = h.meta;
    //         entry.user_meta = h.user_mata;
    //         entry.cas_counter = h.cas_counter;
    //         entry.cas_counter_check = h.cas_counter_check;
    //         let ok = fd.read_u32::<BigEndian>();
    //         if is_eof(&ok) {
    //             break;
    //         }
    //         let crc = ok?;
    //
    //         let mut vp = ValuePointer::default();
    //         vp.len = Header::encoded_size() as u32 + h.k_len + h.v_len + 4;
    //         record_offset += vp.len;
    //
    //         vp.offset = entry.offset;
    //         vp.fid = self.fid;
    //
    //         let _continue = f(&entry, &vp).await?;
    //         if !_continue {
    //             break;
    //         }
    //     }
    //
    //     // todo add truncate
    //     Ok(())
    // }
    pub(crate) fn reset_seek_start(&mut self) -> Result<()> {
        let fd = self.fd.as_mut().unwrap();
        fd.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

#[test]
fn concurrency() {
    let mut lf = LogFile::new("src/test_data/vlog_file.text");
    assert!(lf.is_ok(), "{}", lf.unwrap_err().to_string());
}
