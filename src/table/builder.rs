use crate::y::{hash, is_eof, Decode, Encode, ValueStruct};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use growable_bloom_filter::GrowableBloom;
use log::{debug, info};
use serde_json;

use std::hash::Hasher;
use std::io::{Cursor, Read, Write};

#[derive(Clone, Default, Debug)]
pub(crate) struct Header {
    pub(crate) p_len: u16, // Overlap with base key(Prefix length)
    pub(crate) k_len: u16, // Length of the diff. Eg: "d" = "abcd" - "abc"
    pub(crate) v_len: u16, // Length of the value.
    pub(crate) prev: u32, // Offset for the previous key-value pair. The offset is relative to `block` base offset.
}

impl Header {
    pub(crate) const fn size() -> usize {
        10
    }

    pub(crate) fn is_dummy(&self) -> bool {
        self.k_len == 0 && self.p_len == 0
    }
}

impl Decode for Header {
    fn dec(&mut self, rd: &mut dyn Read) -> crate::Result<()> {
        self.p_len = rd.read_u16::<BigEndian>()?;
        self.k_len = rd.read_u16::<BigEndian>()?;
        self.v_len = rd.read_u16::<BigEndian>()?;
        self.prev = rd.read_u32::<BigEndian>()?;
        Ok(())
    }
}

impl Encode for Header {
    fn enc(&self, wt: &mut dyn Write) -> crate::Result<usize> {
        wt.write_u16::<BigEndian>(self.p_len)?;
        wt.write_u16::<BigEndian>(self.k_len)?;
        wt.write_u16::<BigEndian>(self.v_len)?;
        wt.write_u32::<BigEndian>(self.prev)?;
        Ok(Header::size())
    }
}

impl From<&[u8]> for Header {
    fn from(buffer: &[u8]) -> Self {
        let mut header = Header::default();
        Decode::dec(&mut header, &mut Cursor::new(buffer)).unwrap();
        header
    }
}

impl Into<Vec<u8>> for Header {
    fn into(self) -> Vec<u8> {
        let mut wt = Cursor::new(vec![0u8; Header::size()]);
        Encode::enc(&self, &mut wt).unwrap();
        wt.into_inner()
    }
}

// Used in building a table.
pub struct Builder {
    counter: usize,       // Number of keys written for the current block.
    buf: Cursor<Vec<u8>>, // bytes buffer
    base_key: Vec<u8>,    // Base key for the current block.
    base_offset: u32,     // Offset for the current block.
    restarts: Vec<u32>,   // Base offsets of every block.
    prev_offset: u32, // Tracks offset for the previous key-value-pair. Offset is relative to block base offset.
    key_buf: Cursor<Vec<u8>>,
    key_count: u32,
}

impl Builder {
    // the max keys number of every block.
    pub(crate) const RESTART_INTERVAL: usize = 100;
    pub(crate) fn empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Returns a suffix of new_key that is different from b.base_key.
    fn key_diff<'a>(new_key: &'a [u8], key: &'a [u8]) -> &'a [u8] {
        let mut i = 0usize;
        while i < new_key.len() && i < key.len() {
            if new_key[i] != key[i] {
                break;
            }
            i += 1;
        }
        &new_key[i..]
    }

    fn add_helper(&mut self, key: &[u8], v: &ValueStruct) {
        // Add key to bloom filter.
        self.key_buf
            .write_u16::<BigEndian>(key.len() as u16)
            .unwrap();
        self.key_buf.write_all(key).unwrap();
        self.key_count += 1;

        // diff_key stores the difference of key with base_key.
        let diff_key;
        if self.base_key.is_empty() {
            // Make a copy. Builder should not keep references. Otherwise, caller has to be very careful
            // and will have to make copies of keys every time they add to builder. which is even worse.
            self.base_key.clear();
            self.base_key.extend_from_slice(key);
            diff_key = key;
        } else {
            diff_key = Self::key_diff(key, self.base_key.as_slice());
        }
        let h = Header {
            p_len: (key.len() - diff_key.len()) as u16,
            k_len: diff_key.len() as u16,
            v_len: (v.value.len() + ValueStruct::header_size()) as u16,
            prev: self.prev_offset, // prevOffset is the location of the last key-value added.
        };
        // Remember current offset for the next Add call.
        self.prev_offset = self.buf.get_ref().len() as u32 - self.base_offset;

        // Layout: header, diff_key, value.
        self.buf
            .write_all(<Header as Into<Vec<u8>>>::into(h).as_slice())
            .unwrap();
        self.buf.write_all(diff_key).unwrap();
        self.buf
            .write_all(<&ValueStruct as Into<Vec<u8>>>::into(v).as_slice())
            .unwrap();
        // info!("insert a key-value: {:?}", String::from_utf8_lossy(key));
        // Increment number of keys added for this current block.
        self.counter += 1;
    }

    // Add a key-value pair that indicates the end of a block. The key and value for this pair should both be empty.
    fn finish_block(&mut self) {
        // When we are at the end of the block and Valid=false, and the user wants to do a Prev,
        // we need a dummy header to tell us the offset of the previous key-value pair.
        self.add_helper(b"", &ValueStruct::default());
    }

    /// Add adds a key-value pair to the block.
    /// If doNotRestart is true, we will not restart even if b.counter >= restartInterval.
    pub fn add(&mut self, key: &[u8], value: &ValueStruct) -> crate::y::Result<()> {
        if self.counter >= Self::RESTART_INTERVAL {
            self.finish_block();
            debug!(
                "create new block, base:{:<10}, pre: {:5}, base-key: {:?}",
                self.base_offset,
                self.prev_offset,
                String::from_utf8_lossy(&self.base_key)
            );
            // Start a new block. Initialize the block.
            self.restarts.push(self.buf.get_ref().len() as u32);
            self.counter = 0;
            self.base_key.clear();
            self.base_offset = self.buf.get_ref().len() as u32;
            // First key-value pair of block has header.prev=MaxInt.
            self.prev_offset = u32::MAX;
        }
        self.add_helper(key, value);
        Ok(())
    }

    // TODO: vvv this was the comment on ReachedCapacity.
    // FinalSize returns the *rough* final size of the array, counting the header which is not yet written.
    // TODO: Look into why there is a discrepancy. I suspect it is because of Write(empty, empty)
    // at the end. The diff can vary.

    // ReachedCapacity returns true if we... roughly (?) reached capacity?
    pub(crate) fn reached_capacity(&self, cap: u64) -> bool {
        let estimate_sz =
            self.buf.get_ref().len() + 8 /* empty header */ + 4*self.restarts.len() + 8;
        // 8 = end of buf offset + len(restarts).
        estimate_sz as u64 > cap
    }

    // blockIndex generates the block index for the table.
    // It is mainly a list of all the block base offsets.
    fn block_index(&mut self) -> Vec<u8> {
        // Store the end offset, so we know the length of the final block.
        self.restarts.push(self.buf.get_ref().len() as u32);

        // Add 4 because we want to write out number of restarts at the end.
        let sz = 4 * self.restarts.len() + 4;
        let mut wt = Cursor::new(vec![0u8; sz]);
        for restart in self.restarts.iter() {
            wt.write_u32::<BigEndian>(*restart).unwrap();
        }
        wt.write_u32::<BigEndian>(self.restarts.len() as u32)
            .unwrap();
        let out = wt.into_inner();
        info!("write restart: {:?}", self.restarts);
        assert_eq!(out.len(), sz);
        out
    }

    /// Finishes the table by appending the index.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut bf = GrowableBloom::new(0.01, self.counter);
        loop {
            let kl = self.key_buf.read_u16::<BigEndian>();
            if is_eof(&kl) {
                break;
            }
            if kl.is_err() {
                panic!("{:?}", &kl.unwrap_err());
            }
            let kl = kl.unwrap();
            let mut hash_buffer = vec![0u8; kl as usize];
            self.key_buf.read(&mut hash_buffer).unwrap();
            bf.insert(&hash(&hash_buffer));
        }
        // This will never start a new block.
        self.finish_block();
        let index = self.block_index();
        self.buf.write_all(&index).unwrap();

        // Write bloom filter
        let bdata = serde_json::to_vec(&bf).unwrap();
        println!("{}", serde_json::to_string(&bf).unwrap());
        self.buf.write_all(&bdata).unwrap();
        self.buf.write_u32::<BigEndian>(bdata.len() as u32).unwrap();
        self.buf.get_ref().clone()
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            counter: 0,
            buf: Cursor::new(Vec::with_capacity(64 << 20)),
            base_key: vec![],
            base_offset: 0,
            restarts: vec![],
            prev_offset: u32::MAX,
            key_buf: Cursor::new(Vec::with_capacity(32 << 20)),
            key_count: 0,
        }
    }
}
