use crate::value_log::{Entry, Header, ValuePointer};
use crate::y::Result;
use crate::y::{is_eof, read_at, Decode};
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt};
use memmap::MmapMut;
use parking_lot::lock_api::{RwLockReadGuard, RwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug)]
pub(crate) struct LogFile {
    pub(crate) _path: Box<String>,
    pub(crate) fd: Option<File>,
    pub(crate) fid: u32,
    pub(crate) _mmap: Option<MmapMut>,
    pub(crate) sz: u32,
}

pub(crate) struct SafeLogFile(RwLock<LogFile>);

impl SafeLogFile {
    pub(crate) fn new(log_file: LogFile) -> Self {
        Self(RwLock::new(log_file))
    }
    pub(crate) fn rl(&self) -> RwLockReadGuard<'_, RawRwLock, LogFile> {
        self.0.read()
    }
    pub(crate) fn wl(&self) -> RwLockWriteGuard<'_, RawRwLock, LogFile> {
        self.0.write()
    }
}

impl LogFile {
    fn open_read_only(&mut self) -> Result<()> {
        let mut fd = std::fs::OpenOptions::new()
            .read(true)
            .open(self._path.as_ref())?;
        let meta = fd.metadata()?;
        let file_sz = meta.len();
        let mut _mmap = MmapMut::map_anon(file_sz as usize).unwrap();
        let read = read_at(&fd, &mut _mmap, 0)?;
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
        if offset >= sz as u32 || offset + value_sz > sz as u32 {
            return Err(Error::EOF);
        } else {
            return Ok(&self._mmap.as_ref().unwrap()[offset as usize..(offset + value_sz) as usize]);
        }
        // todo add metrics
    }

    // todo opz
    fn done_writing(&mut self, offset: u32) -> Result<()> {
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

    fn iterate(
        &mut self,
        offset: u32,
        mut f: impl FnMut(&Entry, &ValuePointer) -> Result<bool>,
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

            let _continue = f(&entry, &vp)?;
            if !_continue {
                break;
            }
        }

        // todo add truncate
        Ok(())
    }
}
