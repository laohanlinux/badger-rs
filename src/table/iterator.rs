use crate::table::builder::Header;
use crate::table::table::{Block, Table, TableCore};
use crate::y::{Result, ValueStruct};
use crate::{y, Error};
use std::borrow::Borrow;
use std::cell::{Ref, RefCell, RefMut};
use std::ops::Deref;
use std::{cmp, io};

pub enum IteratorSeek {
    Origin,
    Current,
}

/// Block iterator
#[derive(Debug)]
pub struct BlockIterator {
    data: Vec<u8>,
    pos: RefCell<u32>,
    base_key: RefCell<Vec<u8>>,
    last_header: RefCell<Option<Header>>,
}

pub struct BlockIteratorItem<'a> {
    key: Vec<u8>,
    value: &'a [u8],
}

impl<'a> BlockIteratorItem<'a> {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        self.value
    }
}

impl BlockIterator {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            pos: RefCell::new(0),
            base_key: RefCell::new(vec![]),
            last_header: RefCell::new(None),
        }
    }

    /// Brings us to the first block element that is >= input key.
    pub fn seek(&self, key: &[u8], whence: IteratorSeek) -> Option<BlockIteratorItem> {
        match whence {
            IteratorSeek::Origin => self.reset(),
            IteratorSeek::Current => {}
        }
        while let Some(item) = self.next() {
            if item.key() >= key {
                return Some(item);
            }
        }
        None
    }

    /// Brings us to the first block element that is <= input key.
    pub fn seek_rewind(&self, key: &[u8], whence: IteratorSeek) -> Option<BlockIteratorItem> {
        match whence {
            IteratorSeek::Origin => self.reset(),
            IteratorSeek::Current => {}
        }
        while let Some(item) = self.next() {
            if item.key() <= key {
                return Some(item);
            }
        }
        None
    }

    fn seek_to_first(&self) -> Option<BlockIteratorItem> {
        self.reset();
        self.next()
    }

    fn seek_to_last(&self) -> Option<BlockIteratorItem> {
        while let Some(_) = self.next() {}
        self.prev()
    }

    fn reset(&self) {
        *self.pos.borrow_mut() = 0;
        self.base_key.borrow_mut().clear();
        self.last_header.borrow_mut().take();
    }

    pub(crate) fn next(&self) -> Option<BlockIteratorItem> {
        let mut pos = self.pos.borrow_mut();
        if *pos >= self.data.len() as u32 {
            return None;
        }
        //load header
        let h = Header::from(&self.data[*pos as usize..*pos as usize + Header::size()]);
        *self.last_header.borrow_mut() = Some(h.clone());
        //move pos cursor
        *pos += Header::size() as u32;

        // dummy header
        if h.is_dummy() {
            return None;
        }

        // Populate baseKey if it isn't set yet. This would only happen for the first Next.
        if self.base_key.borrow().is_empty() {
            // This should be the first Next() for this block. Hence, prefix length should be zero.
            assert_eq!(h.p_len, 0);
            self.base_key
                .borrow_mut()
                .extend_from_slice(&self.data[*pos as usize..(*pos + h.k_len as u32) as usize]);
        }
        // drop pos advoid to borrow twice
        drop(pos);
        let (key, value) = self.parse_kv(&h);
        Some(BlockIteratorItem { key, value })
    }

    fn prev(&self) -> Option<BlockIteratorItem> {
        // uninit init
        if self.last_header.borrow().is_none() {
            return None;
        }
        // fist key
        if self.last_header.borrow().as_ref().unwrap().prev == u32::MAX {
            // This is the first element of the block!
            *self.pos.borrow_mut() = 0;
            return None;
        }
        // Move back using current header's prev.
        *self.pos.borrow_mut() = self.last_header.borrow().as_ref().unwrap().prev;
        let h = Header::from(
            &self.data[*self.pos.borrow() as usize..*self.pos.borrow() as usize + Header::size()],
        );
        *self.last_header.borrow_mut() = Some(h.clone());
        *self.pos.borrow_mut() += Header::size() as u32;
        let (key, value) = self.parse_kv(&h);
        Some(BlockIteratorItem { key, value })
    }

    pub(crate) fn _next(&self) -> Option<BlockIteratorItem> {
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

pub struct IteratorItem {
    key: Vec<u8>,
    value: ValueStruct,
}

/// An iterator for a table.
pub struct Iterator<'a> {
    table: &'a TableCore,
    bpos: RefCell<usize>,
    bi: RefCell<Option<BlockIterator>>,
    // Internally, Iterator is bidirectional. However, we only expose the
    // unidirectional functionality for now.
    reversed: bool,
}

impl<'a> y::iterator::Iterator for Iterator<'a> {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.reversed {
            self.next()
        } else {
            self.prev()
        }
    }

    fn rewind(&self) -> Option<Self::Output> {
        if !self.reversed {
            self.seek_to_first()
        } else {
            self.seek_to_last()
        }
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        if !self.reversed {
            self._seek(key)
        } else {
            self.seek_for_prev(key)
        }
    }

    fn valid(&self) -> bool {
        todo!()
    }

    fn close(&self) {
        todo!()
    }
}

impl<'a> Iterator<'a> {
    pub fn seek_to_first(&self) -> Option<IteratorItem> {
        if self.table.block_index.is_empty() {
            return None;
        }
        *self.bpos.borrow_mut() = 0;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow());
        bi.as_ref()
            .unwrap()
            .seek_to_first()
            .map(|b_item| b_item.into())
    }

    pub fn seek_to_last(&self) -> Option<IteratorItem> {
        if self.table.block_index.is_empty() {
            return None;
        }
        *self.bpos.borrow_mut() = self.table.block_index.len() - 1;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow());
        bi.as_ref()
            .unwrap()
            .seek_to_last()
            .map(|b_item| b_item.into())
    }

    fn _seek(&self, key: &[u8]) -> Option<IteratorItem> {
        self.seek_from(key, IteratorSeek::Origin)
    }

    // Brings us to a key that is >= input key.
    // todo maybe reset bpos
    fn seek_from(&self, key: &[u8], whence: IteratorSeek) -> Option<IteratorItem> {
        match whence {
            IteratorSeek::Origin => self.reset(),
            IteratorSeek::Current => {}
        }

        let idx = self
            .table
            .block_index
            .binary_search_by(|ko| ko.key.as_slice().cmp(key));
        if idx.is_ok() {
            return self.seek_helper(idx.unwrap(), key);
        }

        // not found
        let idx = idx.err().unwrap();

        if idx == 0 {
            return self.seek_helper(idx, key);
        }

        if idx > self.table.block_index.len() {
            return self.seek_helper(idx - 1, key);
        }

        //[ (5,6,7), (10, 11), (12, 15)], find 6, check (5,6,7) ~ (10, 11),
        let item = self.seek_helper(idx - 1, key);
        if item.is_some() {
            return item;
        }
        self.seek_helper(idx, key)
    }

    // maybe reset bpos
    fn pre_from(&self, key: &[u8], whence: IteratorSeek) -> Option<IteratorItem> {
        match whence {
            IteratorSeek::Origin => self.reset(),
            IteratorSeek::Current => {}
        }

        let idx = self
            .table
            .block_index
            .binary_search_by(|ko| ko.key.as_slice().cmp(key));
        if idx.is_ok() {
            return self.seek_helper_rewind(idx.unwrap(), key);
        }

        // not found
        let idx = idx.err().unwrap();
        if idx == 0 {
            return None;
        }
        self.seek_helper_rewind(idx - 1, key)
    }

    // brings us to a key that is >= input key.
    fn seek(&self, key: &[u8]) -> Option<IteratorItem> {
        self.seek_from(key, IteratorSeek::Origin)
    }

    // will reset iterator and seek to <= key.
    fn seek_for_prev(&self, key: &[u8]) -> Option<IteratorItem> {
        // TODO: Optimize this. We shouldn't have to take a Prev step.
        if let Some(item) = self.seek_from(key, IteratorSeek::Origin) {
            if item.key == key {
                return Some(item);
            }
            return self.prev();
        }
        None
    }

    fn prev(&self) -> Option<IteratorItem> {
        let nil_bi = self.bi.borrow_mut().is_none();
        if nil_bi {
            assert_eq!(*self.bpos.borrow(), 0);
            // uninit
            let bi = self.get_or_set_bi(*self.bpos.borrow());
            let item = bi.as_ref().unwrap().seek_to_last();
            return item.map(|b_item| b_item.into());
        }
        let item = self
            .bi
            .borrow()
            .as_ref()
            .unwrap()
            .prev()
            .map(|b_item| b_item.into());
        if item.is_some() {
            return item;
        }
        // move to first block indicate not data for match
        if *self.bpos.borrow() == 0 {
            return None;
        }
        *self.bpos.borrow_mut() -= 1;
        self.prev()
    }

    // fn recursion_pre(bpos: isize, table: &TableCore, key: &[u8]) -> Option<BlockIteratorItem> {
    //     if bpos < 0 {
    //         return None;
    //     }
    //     let block = table.block(bpos as usize).unwrap();
    //     let bi = BlockIterator::new(block.data);
    //     bi.seek(key)
    // }

    fn _next(&self) -> Option<IteratorItem> {
        let bpos = self.bpos.borrow_mut();
        if *bpos >= self.table.block_index.len() {
            return None;
        }
        let bi = self.get_or_set_bi(*bpos);
        bi.as_ref().unwrap().next().map(|b_item| b_item.into())
    }

    fn reset(&self) {
        *self.bpos.borrow_mut() = 0;
    }

    // >= key
    fn seek_helper(&self, block_index: usize, key: &[u8]) -> Option<IteratorItem> {
        *self.bpos.borrow_mut() = block_index;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow());
        bi.as_ref()
            .unwrap()
            .seek(key, IteratorSeek::Origin)
            .map(|b_item| b_item.into())
    }

    fn seek_helper_rewind(&self, block_index: usize, key: &[u8]) -> Option<IteratorItem> {
        *self.bpos.borrow_mut() = block_index;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow());
        bi.as_ref()
            .unwrap()
            .seek_rewind(key, IteratorSeek::Origin)
            .map(|b_item| b_item.into())
    }

    fn get_bi(&self) -> &Option<&BlockIterator> {
        if self.bi.borrow().is_none() {
            return &None;
        }
        let bi = self.bi.as_ptr() as *const Option<&BlockIterator>;
        unsafe { &*bi }
    }

    fn get_or_set_bi(&self, bpos: usize) -> RefMut<'_, Option<BlockIterator>> {
        let mut bi = self.bi.borrow_mut();
        if bi.is_some() {
            return bi;
        }
        let mut block = self.table.block(bpos).unwrap();
        let it = BlockIterator::new(block.data);
        *bi = Some(it);
        bi
    }

    fn get_bi_by_bpos(&self, bpos: usize) -> RefMut<'_, Option<BlockIterator>> {
        let mut bi = self.bi.borrow_mut();
        let mut block = self.table.block(bpos).unwrap();
        let it = BlockIterator::new(block.data);
        *bi = Some(it);
        bi
    }
}

impl<'a> From<BlockIteratorItem<'a>> for IteratorItem {
    fn from(b_item: BlockIteratorItem) -> Self {
        let key = b_item.key;
        let value = ValueStruct::from(b_item.value);
        IteratorItem { key, value }
    }
}

/// concatenates the sequences defined by several iterators.  (It only works with
/// TableIterators, probably just because it's faster to not be so generic.)
pub struct ConcatIterator<'a> {
    index: usize, // Which iterator is active now.
    cur: &'a Iterator<'a>,
    iters: &'a [&'a Iterator<'a>], // Corresponds to `tables`.
    tables: &'a [&'a Table], // Disregarding `reversed`, this is in ascending order.
    reversed: bool,
}

#[test]
fn it() {
    let n = vec![2, 19, 30, 31, 33, 34, 35, 37];
    println!("{:?}", n.binary_search_by(|a| a.cmp(&1)));
}
