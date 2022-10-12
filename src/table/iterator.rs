use crate::table::builder::Header;
use crate::table::table::Table;
use crate::y::{Result, ValueStruct};
use crate::Error;
use std::cell::RefCell;
use std::ops::Deref;
use std::{cmp, io};

#[derive(Debug)]
pub(crate) struct BlockIterator<'a> {
    data: &'a [u8],
    pos: RefCell<u32>,
    base_key: RefCell<&'a [u8]>,
    last_header: RefCell<Header>,
}

pub(crate) struct BlockIteratorItem<'a> {
    key: Vec<u8>,
    value: &'a [u8],
}

impl<'a> BlockIteratorItem<'a> {
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    pub(crate) fn value(&self) -> &[u8] {
        self.value
    }
}

pub(crate) enum BlockIteratorSeek {
    Origin,
    Current,
}

impl<'a> BlockIterator<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: RefCell::new(0),
            base_key: RefCell::new(&data[..0]),
            last_header: RefCell::new(Header::default()),
        }
    }

    pub(crate) fn seek(
        &'a self,
        key: &[u8],
        whence: BlockIteratorSeek,
    ) -> Option<BlockIteratorItem<'a>> {
        match whence {
            BlockIteratorSeek::Origin => self.reset(),
            BlockIteratorSeek::Current => {}
        }
        while let Some(item) = self.next() {
            if item.key() == key {
                return Some(item);
            }
        }
        None
    }

    fn seek_to_first(&'a self) -> Option<BlockIteratorItem<'a>> {
        self.reset();
        self.next()
    }

    fn seek_to_last(&'a self) -> Option<BlockIteratorItem<'a>> {
        while let Some(_) = self.next() {}
        self.prev()
    }

    fn prev(&'a self) -> Option<BlockIteratorItem<'a>> {
        // todo Why?
        if self.last_header.borrow().prev == u32::MAX {
            return None;
        }
        // Move back using current header's prev.
        *self.pos.borrow_mut() = self.last_header.borrow().prev;
        let h = Header::from(
            &self.data[*self.pos.borrow() as usize..*self.pos.borrow() as usize + Header::size()],
        );
        *self.last_header.borrow_mut() = h.clone();
        *self.pos.borrow_mut() += Header::size() as u32;
        let item = self.parse_kv(&h);
        Some(BlockIteratorItem {
            key: item.0,
            value: item.1,
        })
    }

    fn reset(&self) {
        *self.pos.borrow_mut() = 0;
        *self.base_key.borrow_mut() = &self.base_key.borrow().deref()[..0];
    }

    pub(crate) fn next(&'a self) -> Option<BlockIteratorItem<'a>> {
        let mut pos = self.pos.borrow_mut();
        if *pos >= self.data.len() as u32 {
            return None;
        }

        let h = Header::from(&self.data[*pos as usize..*pos as usize + Header::size()]);
        *self.last_header.borrow_mut() = h.clone();
        *pos += Header::size() as u32;

        // dummy header
        if h.k_len == 0 && h.p_len == 0 {
            return None;
        }

        // Populate baseKey if it isn't set yet. This would only happen for the first Next.
        if self.base_key.borrow().is_empty() {
            // This should be the first Next() for this block. Hence, prefix length should be zero.
            assert_eq!(h.p_len, 0);
            *self.base_key.borrow_mut() =
                &self.data[*pos as usize..(*pos + h.k_len as u32) as usize];
        }
        drop(pos);
        let (key, value) = self.parse_kv(&h);
        Some(BlockIteratorItem { key, value })
    }

    pub(crate) fn _next(&'a self) -> Option<BlockIteratorItem<'a>> {
        self.next()
    }

    fn parse_kv(&self, h: &Header) -> (Vec<u8>, &[u8]) {
        let mut pos = self.pos.borrow_mut();
        let mut key = vec![0u8; (h.p_len + h.k_len) as usize];
        key[..h.p_len as usize].copy_from_slice(&self.base_key.borrow()[..h.p_len as usize]);
        key[h.p_len as usize..]
            .copy_from_slice(&self.data[*pos as usize..*pos as usize + h.k_len as usize]);
        *pos += h.k_len as u32;
        assert!(
            *pos as usize + h.v_len as usize <= self.data.len(),
            "Value exceeded size of block: {} {} {} {} {:?}",
            pos,
            h.k_len,
            h.v_len,
            self.data.len(),
            h
        );

        let value = &self.data[*pos as usize..*pos as usize + h.v_len as usize];
        *pos += h.v_len as u32;
        (key, value)
    }
}

// impl<'a> std::iter::Iterator for BlockIterator<'a> {
//     type Item = BlockIteratorItem<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         self._next()
//     }
// }

struct Iterator<'a> {
    table: &'a Table,
    bpos: usize,
    bi: &'a BlockIterator<'a>,
    reversed: bool,
}

impl<'a> Iterator<'a> {
    fn close(&mut self) {
        self.table.deref();
    }
    fn reset(&mut self) {
        self.bpos = 0;
    }

    fn valid(&self) -> bool {
        todo!()
    }

    fn seek_to_first(&self) {
        todo!()
    }

    fn seek_to_last(&self) {
        todo!()
    }

    fn seek_helper(&self, block_index: usize, key: &[u8]) {
        todo!()
    }

    fn seek_from(&self, key: &[u8], whence: BlockIteratorSeek) {
        todo!()
    }

    fn _seek(&self, key: &[u8]) {
        todo!()
    }

    fn seek_for_prev(&self, key: &[u8]) {
        todo!()
    }

    fn _next(&self) {
        todo!()
    }

    fn prev(&self) {
        todo!()
    }

    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> ValueStruct {
        todo!()
    }

    fn next(&self) {
        todo!()
    }

    fn rewind(&self) {
        todo!()
    }

    fn seek(&self) {
        todo!()
    }
}
// concatenates the sequences defined by several iterators.  (It only works with
// TableIterators, probably just because it's faster to not be so generic.)
struct ConcatIterator<'a> {
    index: usize, // Which iterator is active now.
    cur: &'a Iterator<'a>,
    iters: &'a [&'a Iterator<'a>],
    tables: &'a [&'a Table],
    reversed: bool,
}

#[test]
fn it() {}
