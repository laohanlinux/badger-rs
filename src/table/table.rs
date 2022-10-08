use crate::options::FileLoadingMode;
use crate::options::FileLoadingMode::MemoryMap;
use crate::table::builder::Header;
use crate::y::{hash, mmap, Result};
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt};
use filename::file_name;
use growable_bloom_filter::GrowableBloom;
use memmap::Mmap;
use std::fs::{remove_file, File};
use std::io;
use std::io::{Cursor, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

const FILE_SUFFIX: &str = "";

#[derive(Clone)]
struct KeyOffset {
    key: Vec<u8>,
    offset: usize,
    len: usize,
}

pub struct Table {}

struct TableCore {
    _ref: AtomicI32,
    fd: File,
    file_name: String,
    // Initialized in OpenTable, using fd.Stat()
    table_size: usize,
    block_index: Vec<KeyOffset>,
    loading_mode: FileLoadingMode,
    _mmap: Option<Mmap>, // Memory mapped.
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
    pub fn open_table(mut fd: File, loading_mode: FileLoadingMode) -> Result<Self> {
        let filename = file_name(&fd)
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let file_sz = fd.seek(SeekFrom::End(0)).or_else(Err)?;
        fd.seek(SeekFrom::Start(0)).or_else(Err)?;
        let id = parse_file_id(&filename)?;
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
        if loading_mode == MemoryMap {
            table._mmap = Some(mmap(&table.fd, false, file_sz as usize)?);
        } else {
            table.load_to_ram()?;
        }
        table.read_index()?;

        // todo
        Ok(table)
    }

    // increments the refcount (having to do with whether the file should be deleted)
    fn incr_ref(&self) {
        self._ref.fetch_add(1, Ordering::Relaxed);
    }
    // decrements the refcount and possibly deletes the table
    fn decr_ref(&self) {
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
        let nbr = self.fd.read_at(&mut buffer, off as u64)?;
        // todo add stats
        Ok(buffer)
    }

    fn read_no_fail(&self, off: usize, sz: usize) -> Vec<u8> {
        self.read(off, sz).unwrap()
    }

    // todo
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

        // The last offset stores the end of the last block.
        for i in 0..offsets.len() {
            let index = if i == 0 {
                KeyOffset {
                    offset: 0,
                    len: offsets[i] as usize,
                    key: vec![],
                }
            } else {
                KeyOffset {
                    offset: offsets[i] as usize,
                    len: (offsets[i] - offsets[i - 1]) as usize,
                    key: vec![],
                }
            };
            self.block_index.push(index);
        }
        if self.block_index.len() == 1 {
            return Ok(());
        }

        // TODO use currency
        for (i, block) in self.block_index.clone().iter().enumerate() {
            let buffer = self.read(block.offset, Header::size())?;
            let head = Header::from(buffer.as_slice());
            assert_eq!(
                head.p_len, 0,
                "key offset: {}, h.p_len = {}",
                block.offset, head.p_len
            );
            let out = self.read(Header::size() + block.offset, head.k_len as usize)?;
            self.block_index[i].key = out;
        }
        self.block_index.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(())
    }

    fn block(&self, index: usize) -> Result<Block> {
        if index >= self.block_index.len() {
            return Err("block out of index".into());
        }

        let ko = &self.block_index[index];
        let mut blk = Block {
            offset: ko.offset,
            data: vec![],
        };
        blk.data = self.read(blk.offset, ko.len)?;
        Ok(blk)
    }

    pub fn size(&self) -> usize {
        self.table_size
    }

    pub fn smallest(&self) -> &Vec<u8> {
        &self.smallest
    }

    pub fn biggest(&self) -> &Vec<u8> {
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

    fn load_to_ram(&mut self) -> Result<()> {
        let mut _mmap = vec![0u8; self.table_size];
        let read = self.fd.read_at(&mut _mmap, 0).or_else(Err)?;
        if read != self.table_size {
            return Err(format!(
                "Unable to load file in memory, Table faile: {}",
                self.filename()
            )
            .into());
        }
        // todo stats
        Ok(())
    }
}

impl Drop for TableCore {
    fn drop(&mut self) {
        // We can safely delete this file, because for all the current files, we always have
        // at least one reference pointing to them.

        // It's necessary to delete windows files
        if self.loading_mode == MemoryMap {
            // todo
        }
        self.fd.set_len(0).expect("can not truncate file to 0");
        drop(&mut self.fd);
        remove_file(Path::new(&self.file_name)).expect("fail to remove file");
    }
}

struct Block {
    offset: usize,
    data: Vec<u8>,
}

type ByKey = Vec<KeyOffset>;

pub fn parse_file_id(name: &str) -> Result<u64> {
    use std::str::pattern::Pattern;
    let path = Path::new(name);
    let filename = path.file_name().unwrap().to_str().unwrap();
    if !filename.is_suffix_of(FILE_SUFFIX) {
        return Ok(0);
    }
    let name = filename.trim_end_matches(FILE_SUFFIX);
    name.parse::<u64>()
        .or_else(|err| Err(err.to_string().into()))
}

pub fn id_to_filename(id: u64) -> String {
    format!("{}{}", id, FILE_SUFFIX)
}

pub fn new_file_name(id: u64, dir: String) -> String {
    Path::new(&dir)
        .join(&id_to_filename(id))
        .to_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn t_metrics() {
    // // construct a TaskMonitor
    // let monitor = tokio_metrics::TaskMonitor::new();
    //
    // // print task metrics every 500ms
    // {
    //     let frequency = std::time::Duration::from_millis(500);
    //     let monitor = monitor.clone();
    //     tokio::spawn(async move {
    //         for metrics in monitor.intervals() {
    //             println!("{:?}", metrics);
    //             tokio::time::sleep(frequency).await;
    //         }
    //     });
    // }
    //
    // // instrument some tasks and spawn them
    // loop {
    //     monitor.instrument(async { tokio::time::sleep(Duration::from_millis(20)).await;
    // }
}
