use crate::options::FileLoadingMode;
use crate::options::FileLoadingMode::MemoryMap;
use crate::table::builder::Header;
use crate::y::{hash, mmap, parallel_load_block_key, read_at, Result};
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt};

use growable_bloom_filter::GrowableBloom;
use memmap::MmapMut;

use std::collections::HashSet;

use std::fmt::{Debug, Display, Formatter};
use std::fs::{read_dir, remove_file, File};
use std::io::{Cursor, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::{fmt, io};

#[cfg(target_os = "macos")]
use std::os::unix::fs::FileExt;

use crate::types::{XArc, XWeak};
use crate::y::iterator::Xiterator;

use log::{debug, info};

#[cfg(target_os = "windows")]
use std::os::windows::fs::FileExt;

use std::str::pattern::Pattern;

pub(crate) const FILE_SUFFIX: &str = ".sst";

#[derive(Clone, Debug)]
pub(crate) struct KeyOffset {
    pub(crate) key: Vec<u8>,
    offset: usize,
    len: usize,
}

impl Display for KeyOffset {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyOffset")
            .field("key", &String::from_utf8_lossy(&self.key))
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}

pub type Table = XArc<TableCore>;
pub type WeakTable = XWeak<TableCore>;

impl From<TableCore> for Table {
    fn from(value: TableCore) -> Self {
        Table::new(value)
    }
}

impl Table {
    pub fn incr_ref(&self) {
        self.to_ref().incr_ref()
    }

    pub fn decr_ref(&self) {
        self.to_ref().decr_ref()
    }

    pub fn size(&self) -> usize {
        self.to_ref().size()
    }

    pub fn biggest(&self) -> &[u8] {
        &self.biggest
    }

    pub fn smallest(&self) -> &[u8] {
        &self.smallest
    }
}

pub struct TableCore {
    _ref: AtomicI32,
    fd: File,
    file_name: String,
    // Initialized in OpenTable, using fd.Stat()
    table_size: usize,
    pub(crate) block_index: Vec<KeyOffset>,
    loading_mode: FileLoadingMode,
    _mmap: Option<MmapMut>, // Memory mapped.
    // The following are initialized once and const.
    smallest: Vec<u8>, // smallest keys.
    biggest: Vec<u8>,  // biggest keys.
    id: u64,
    bf: GrowableBloom,
}

impl TableCore {
    // assumes file has only one table and opens it.  Takes ownership of fd upon function
    // entry.  Returns a table with one reference count on it (decrementing which may delete the file!
    // -- consider t.Close() instead).  The fd has to writeable because we call Truncate on it before
    // deleting.
    pub fn open_table(mut fd: File, filename: &str, loading_mode: FileLoadingMode) -> Result<Self> {
        let file_sz = fd.seek(SeekFrom::End(0)).or_else(Err)?;
        fd.seek(SeekFrom::Start(0)).or_else(Err)?;
        let id = parse_file_id(filename)?;
        let mut table = TableCore {
            _ref: AtomicI32::new(1),
            fd,
            file_name: filename.to_string(),
            table_size: file_sz as usize,
            block_index: vec![],
            loading_mode,
            _mmap: None,
            smallest: vec![],
            biggest: vec![],
            id,
            bf: GrowableBloom::new(0.01, 1),
        };

        #[cfg(any(target_os = "macos", target_os = "linux"))]
        if loading_mode == MemoryMap {
            table._mmap = Some(mmap(&table.fd, false, file_sz as usize)?);
        } else {
            table.load_to_ram()?;
        }

        #[cfg(any(target_os = "windows"))]
        table.load_to_ram()?;

        table.read_index()?;
        let table_ref = Table::new(table);
        let biggest = {
            let iter1 = super::iterator::IteratorImpl::new(table_ref.clone(), true);
            iter1
                .rewind()
                .map(|item| item.key().to_vec())
                .or_else(|| Some(vec![]))
        }
        .unwrap();

        let smallest = {
            let iter1 = super::iterator::IteratorImpl::new(table_ref.clone(), false);
            iter1
                .rewind()
                .map(|item| item.key().to_vec())
                .or_else(|| Some(vec![]))
        }
        .unwrap();
        let mut tc = table_ref.to_inner().unwrap();
        tc.biggest = biggest;
        tc.smallest = smallest;
        info!("open table ==> {}", tc);
        Ok(tc)
    }

    // increments the refcount (having to do with whether the file should be deleted)
    pub(crate) fn incr_ref(&self) {
        self._ref.fetch_add(1, Ordering::Relaxed);
    }
    // decrements the refcount and possibly deletes the table
    pub(crate) fn decr_ref(&self) {
        self._ref.fetch_sub(1, Ordering::Relaxed);
    }

    fn close(&mut self) {
        todo!()
    }
}

impl TableCore {
    fn read(&self, off: usize, sz: usize) -> Result<Vec<u8>> {
        if let Some(m) = self._mmap.as_ref() {
            if !m.is_empty() {
                if m[off..].len() < sz {
                    return Err(Error::Io(io::ErrorKind::UnexpectedEof.to_string()));
                }
                return Ok(m[off..off + sz].to_vec());
            }
        }
        let mut buffer = vec![0u8; sz];
        read_at(&self.fd, &mut buffer, sz as u64)?;
        // todo add stats
        Ok(buffer)
    }

    fn read_no_fail(&self, off: usize, sz: usize) -> Vec<u8> {
        self.read(off, sz).unwrap()
    }

    // TODO maybe use &self
    fn read_index(&mut self) -> Result<()> {
        let mut read_pos = self.table_size;
        // Read bloom filter.
        read_pos -= 4;
        let buf = self.read_no_fail(read_pos, 4);
        let bloom_len = Cursor::new(buf).read_u32::<BigEndian>().unwrap();
        read_pos -= bloom_len as usize;
        let data = self.read_no_fail(read_pos, bloom_len as usize);
        self.bf = serde_json::from_slice(&data).unwrap();

        read_pos -= 4;
        let restarts_len = Cursor::new(self.read_no_fail(read_pos, 4))
            .read_u32::<BigEndian>()
            .unwrap();

        read_pos -= 4 * restarts_len as usize;
        let mut buf = Cursor::new(self.read_no_fail(read_pos, 4 * restarts_len as usize));

        let mut offsets = vec![0u32; restarts_len as usize];
        for i in 0..restarts_len as usize {
            offsets[i] = buf.read_u32::<BigEndian>().unwrap();
        }

        // println!("restart {:?}", offsets);
        // The last offset stores the end of the last block.
        for i in 0..offsets.len() {
            let offset = {
                if i == 0 {
                    0
                } else {
                    offsets[i - 1]
                }
            };
            let index = KeyOffset {
                offset: offset as usize,
                len: (offsets[i] - offset) as usize,
                key: vec![],
            };
            self.block_index.push(index);
        }
        // todo Why reload key
        if self.block_index.len() == 1 {
            return Ok(());
        }

        if self._mmap.is_some() {
            for (i, block) in self.block_index.clone().iter().enumerate() {
                let buffer = self.read(block.offset, Header::size())?;
                let head = Header::from(buffer.as_slice());
                assert_eq!(
                    head.p_len, 0,
                    "key offset: {}, h.p_len = {}",
                    block.offset, head.p_len
                );
                let out = self.read(Header::size() + block.offset, head.k_len as usize)?;
                self.block_index[i].key = out.clone().to_vec();
            }
        } else {
            let fp = self.fd.try_clone().unwrap();
            let offsets = self
                .block_index
                .iter()
                .map(|key_offset| key_offset.offset as u64)
                .collect::<Vec<_>>();
            let keys = parallel_load_block_key(fp, offsets);
            for i in 0..keys.len() {
                self.block_index[i].key = keys[i].to_vec();
            }
        }
        self.block_index.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(())
    }

    pub(crate) fn block(&self, index: usize) -> Result<Block> {
        if index >= self.block_index.len() {
            return Err("block out of index".into());
        }
        let ko = &self.block_index[index];
        let data = self.read(ko.offset, ko.len)?;
        Ok(Block {
            offset: ko.offset,
            data,
        })
    }

    pub fn size(&self) -> usize {
        self.table_size
    }

    pub fn smallest(&self) -> &[u8] {
        &self.smallest
    }

    pub fn biggest(&self) -> &[u8] {
        &self.biggest
    }

    pub fn filename(&self) -> &String {
        &self.file_name
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns true if (but not "only if") the table does not have the key. It does a bloom filter lookup.
    pub fn does_not_have(&self, key: &[u8]) -> bool {
        let id = hash(key);
        self.bf.contains(&id)
    }

    /// load to ram that stored with mmap
    fn load_to_ram(&mut self) -> Result<()> {
        let mut _mmap = MmapMut::map_anon(self.table_size).unwrap();
        let read = read_at(&self.fd, &mut _mmap, 0)?;
        if read != self.table_size {
            return Err(format!(
                "Unable to load file in memory, Table faile: {}",
                self.filename()
            )
            .into());
        }
        // todo stats
        self._mmap = Some(_mmap);
        Ok(())
    }
}

impl Drop for TableCore {
    fn drop(&mut self) {
        dbg!(self._ref.load(Ordering::Relaxed));
        // We can safely delete this file, because for all the current files, we always have
        // at least one reference pointing to them.
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        if self.loading_mode == MemoryMap {
            self._mmap
                .as_mut()
                .unwrap()
                .flush()
                .expect("failed to mmap")
        }
        // It's necessary to delete windows files
        // This is very important to let the FS know that the file is deleted.
        #[cfg(not(test))]
        self.fd.set_len(0).expect("can not truncate file to 0");
        #[cfg(not(test))]
        remove_file(Path::new(&self.file_name)).expect("fail to remove file");
    }
}

impl Display for TableCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let index_str = self
            .block_index
            .iter()
            .map(|x| format!("{}", x))
            .collect::<Vec<_>>();
        let smallest = String::from_utf8_lossy(self.smallest());
        let biggest = String::from_utf8_lossy(self.biggest());
        f.debug_struct("Table")
            .field("block_index", &index_str)
            .field("_ref", &self._ref.load(Ordering::Relaxed))
            .field("fname", &self.file_name)
            .field("size", &self.table_size)
            .field("smallest", &smallest)
            .field("biggest", &biggest)
            .finish()
    }
}

pub(crate) struct Block {
    offset: usize,
    pub(crate) data: Vec<u8>,
}

impl Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Block")
            .field("offset", &self.offset)
            .field("len", &self.data.len())
            .finish()
    }
}

type ByKey = Vec<KeyOffset>;

pub fn get_id_map(dir: &str) -> HashSet<u64> {
    let dir = read_dir(dir).unwrap();
    let mut ids = HashSet::new();
    for el in dir {
        if el.is_err() {
            continue;
        }
        let dir_el = el.unwrap();
        if dir_el.metadata().unwrap().is_dir() {
            continue;
        }
        let fid = parse_file_id(dir_el.file_name().to_str().unwrap());
        if fid.is_err() {
            debug!("Skip file, {:?}", fid.unwrap_err());
            continue;
        }
        debug!("What dir : {:?} {:?}", fid, dir_el.file_name());
        ids.insert(fid.unwrap());
    }
    ids
}

pub fn parse_file_id(name: &str) -> Result<u64> {
    use std::str::pattern::Pattern;
    let path = Path::new(name);
    let filename = path.file_name().unwrap().to_str().unwrap();
    if !FILE_SUFFIX.is_suffix_of(filename) {
        return Err(format!("invalid file {}", name).into());
    }
    let name = filename.trim_end_matches(FILE_SUFFIX);
    name.parse::<u64>()
        .or_else(|err| Err(err.to_string().into()))
}

pub fn id_to_filename(id: u64) -> String {
    format!("{}{}", id, FILE_SUFFIX)
}

pub fn new_file_name(id: u64, dir: &str) -> String {
    Path::new(dir)
        .join(&id_to_filename(id))
        .to_str()
        .unwrap()
        .to_string()
}
