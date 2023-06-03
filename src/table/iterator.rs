use crate::table::builder::Header;
use crate::table::table::{Block, Table, TableCore};
use crate::y::iterator::{KeyValue, Xiterator};
use crate::y::{Result, ValueStruct};
use crate::{y, Error};
use log::debug;
use std::borrow::{Borrow, BorrowMut};
use std::cell::{Ref, RefCell, RefMut};
use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Less};
use std::fmt::Formatter;
use std::ops::Deref;
use std::process::id;
use std::ptr::{slice_from_raw_parts, NonNull};
use std::{cmp, fmt, io};
use tracing::info;

pub enum IteratorSeek {
    Origin,
    Current,
}

/// Block iterator
pub struct BlockIterator {
    data: Vec<u8>,
    pos: RefCell<u32>,
    base_key: RefCell<Vec<u8>>,
    last_header: RefCell<Option<Header>>,
    last_block: RefCell<Option<BlockSlice>>,
}

pub struct BlockSlice {
    key: Vec<u8>,
    // todo: use zero copy
    data: *const u8,
    len: usize,
}

impl BlockSlice {
    fn new(key: Vec<u8>, data: &[u8]) -> BlockSlice {
        let ptr = data.as_ptr();
        BlockSlice {
            key,
            data: ptr,
            len: data.len(),
        }
    }

    fn data(&self) -> &[u8] {
        unsafe { &*slice_from_raw_parts(self.data, self.len) }
    }
}

impl fmt::Display for BlockIterator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let base_key = self.base_key.borrow();
        let last_header = self
            .last_header
            .borrow()
            .as_ref()
            .map(|h| format!("{:?}", h))
            .or_else(|| Some("Empty".to_string()))
            .unwrap();
        write!(
            f,
            "biter len:{}, pos:{}, base_key:{}, last_header:[{}]",
            self.data.len(),
            self.pos.borrow(),
            String::from_utf8_lossy(base_key.as_slice()),
            last_header,
        )
    }
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
        #[cfg(test)]
        debug!("build a BlockIterator, buffer len: {}", data.len());

        Self {
            data,
            pos: RefCell::new(0),
            base_key: RefCell::new(vec![]),
            last_header: RefCell::new(None),
            last_block: RefCell::new(None),
        }
    }

    /// Brings us to the first block element that is >= input key.
    pub fn seek(&self, key: &[u8], whence: IteratorSeek) -> Option<BlockIteratorItem> {
        match whence {
            IteratorSeek::Origin => self.reset(),
            IteratorSeek::Current => {}
        }
        while let Some(item) = self.next() {
            // info!("seek {:?}, {:?}", key, item.key());
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

    // todo o shift, too slow, must do something
    fn seek_to_last(&self) -> Option<BlockIteratorItem> {
        while let Some(_) = self.next() {}
        // skip dummy header
        self.prev()
    }

    fn reset(&self) {
        *self.pos.borrow_mut() = 0;
        self.base_key.borrow_mut().clear();
        self.last_header.borrow_mut().take();
        self.last_block.borrow_mut().take();
    }

    // TODO Opz Code
    pub(crate) fn next(&self) -> Option<BlockIteratorItem> {
        let mut pos = self.pos.borrow_mut();
        if *pos >= self.data.len() as u32 {
            return None;
        }
        //load header
        let mut h = Header::from(&self.data[*pos as usize..*pos as usize + Header::size()]);
        *self.last_header.borrow_mut() = Some(h.clone());
        //move pos cursor
        *pos += Header::size() as u32;

        // If the key is dummy, What should be do continue ...
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

    // If not init(next), peek will be return `None`
    pub(crate) fn peek(&self) -> Option<BlockIteratorItem> {
        if self.last_header.borrow().is_none() {
            return None;
        }
        if self.last_header.borrow().as_ref().unwrap().is_dummy() {
            return None;
        }
        // todo advoid use unsafe code
        let b = unsafe { self.last_block.as_ptr().as_ref().unwrap().as_ref().unwrap() };
        Some(BlockIteratorItem {
            key: b.key.clone(),
            value: b.data(),
        })
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
            self.last_header.borrow_mut().take();
            self.last_block.borrow_mut().take();
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
        self.last_block
            .borrow_mut()
            .replace(BlockSlice::new(key.clone(), value));
        (key, value)
    }
}

#[derive(Debug, Clone)]
pub struct IteratorItem {
    pub key: Vec<u8>,
    pub value: ValueStruct,
}

impl fmt::Display for IteratorItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // write!(f, "key: {}, value: {:?}", String::from_utf8_lossy(self.key.as_slice()), self.value())
        write!(f, "key: {:?}, value: {:?}", self.key, self.value())
    }
}

impl IteratorItem {
    pub fn new(key: Vec<u8>, value: ValueStruct) -> IteratorItem {
        IteratorItem { key, value }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
    pub fn value(&self) -> &ValueStruct {
        &self.value
    }
}

/// TODO add start or end
/// An iterator for a table.
pub struct IteratorImpl {
    table: Table,
    bpos: RefCell<isize>,
    // block chunk index
    // start 0 to block.len() - 1
    bi: RefCell<Option<BlockIterator>>,
    // Internally, Iterator is bidirectional. However, we only expose the
    // unidirectional functionality for now.
    reversed: bool,
}

impl fmt::Display for IteratorImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let bi = self.bi.borrow().as_ref().map(|b| format!("{}", b)).unwrap();
        f.debug_struct("IteratorImpl")
            .field("bpos", &self.bpos.borrow())
            .field("bi", &self.bi.borrow().is_some())
            .field("reverse", &self.reversed)
            .finish()
    }
}

impl Xiterator for IteratorImpl {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if !self.reversed {
            self._next()
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

    fn peek(&self) -> Option<Self::Output> {
        if self.bi.borrow().is_none() {
            return None;
        }
        let bi = self.bi.borrow();
        if let Some(itr) = bi.as_ref() {
            itr.peek().map(|block| block.into())
        } else {
            None
        }
    }
}

impl IteratorImpl {
    pub fn new(table: Table, reversed: bool) -> IteratorImpl {
        table.incr_ref(); // Important
        let mut itr = IteratorImpl {
            table,
            bpos: RefCell::new(0),
            bi: RefCell::new(None),
            reversed,
        };
        itr.reset();
        itr
    }

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
        *self.bpos.borrow_mut() = (self.table.block_index.len() - 1) as isize;
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
            return self.seek_helper(idx.unwrap() as isize, key);
        }

        // not found
        let idx = idx.err().unwrap();
        // info!("block_index {:?}, {:?}", idx, self.table.block_index);
        if idx == 0 {
            return self.seek_helper(idx as isize, key);
        }

        if idx >= self.table.block_index.len() {
            return self.seek_helper((idx - 1) as isize, key);
        }

        //[ (5,6,7), (10, 11), (12, 15)], find 6, check (5,6,7) ~ (10, 11),
        let item = self.seek_helper((idx - 1) as isize, key);
        if item.is_some() {
            return item;
        }
        self.seek_helper(idx as isize, key)
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
            return self.seek_helper_rewind(idx.unwrap() as isize, key);
        }

        // not found
        let idx = idx.err().unwrap();
        if idx == 0 {
            return None;
        }
        self.seek_helper_rewind((idx - 1) as isize, key)
    }

    // brings us to a key that is >= input key.
    pub(crate) fn seek(&self, key: &[u8]) -> Option<IteratorItem> {
        self.seek_from(key, IteratorSeek::Origin)
    }

    // will reset iterator and seek to <= key.
    pub(crate) fn seek_for_prev(&self, key: &[u8]) -> Option<IteratorItem> {
        // TODO: Optimize this. We shouldn't have to take a Prev step.
        if let Some(item) = self.seek_from(key, IteratorSeek::Origin) {
            // >= key
            if item.key == key {
                // == key
                return Some(item);
            }
            self.prev() // > key
        } else {
            self.prev() // Just move it front one
        }
    }

    pub(crate) fn prev(&self) -> Option<IteratorItem> {
        let nil_bi = self.bi.borrow_mut().is_none();
        if nil_bi {
            // assert_eq!(*self.bpos.borrow(), 0);
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
        self.bi.borrow_mut().take();
        self.prev()
    }

    fn _next(&self) -> Option<IteratorItem> {
        let mut bpos = self.bpos.borrow_mut();
        if *bpos >= self.table.block_index.len() as isize {
            return None;
        }
        let mut bi = self.get_or_set_bi(*bpos);
        if let Some(item) = bi.as_ref().unwrap().next() {
            return Some(item.into());
        }
        *bpos += 1;
        drop(bpos);
        bi.borrow_mut().take();
        drop(bi);
        self._next()
    }

    /// Note: Reset will move to first item.
    pub(crate) fn reset(&self) {
        if !self.reversed {
            *self.bpos.borrow_mut() = 0;
            self.bi.borrow_mut().take();
        } else {
            *self.bpos.borrow_mut() = (self.table.block_index.len() - 1) as isize;
            self.bi.borrow_mut().take();
        }
    }

    // >= key
    fn seek_helper(&self, block_index: isize, key: &[u8]) -> Option<IteratorItem> {
        *self.bpos.borrow_mut() = block_index;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow());
        bi.as_ref()
            .unwrap()
            .seek(key, IteratorSeek::Origin)
            .map(|b_item| b_item.into())
    }

    fn seek_helper_rewind(&self, block_index: isize, key: &[u8]) -> Option<IteratorItem> {
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
        // todo advoid use unsafe code
        let bi = self.bi.as_ptr() as *const Option<&BlockIterator>;
        unsafe { &*bi }
    }

    fn get_or_set_bi(&self, bpos: isize) -> RefMut<'_, Option<BlockIterator>> {
        assert!(bpos >= 0);
        let mut bi = self.bi.borrow_mut();
        if bi.is_some() {
            return bi;
        }
        let block = self.table.block(bpos as usize).unwrap();
        let it = BlockIterator::new(block.data);
        *bi = Some(it);
        bi
    }

    fn get_bi_by_bpos(&self, bpos: isize) -> RefMut<'_, Option<BlockIterator>> {
        assert!(bpos >= 0);
        let block = self.table.block(bpos as usize).unwrap();
        // info!("===>{:?}, {:?}", bpos, block);
        let mut bi = self.bi.borrow_mut();
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
pub struct ConcatIterator {
    // Index < 0, indicate uninit
    index: RefCell<isize>,
    // Which iterator is active now. todo use usize
    iters: Vec<IteratorImpl>,
    // Corresponds to `tables`.
    tables: Vec<Table>,
    // Disregarding `reversed`, this is in ascending order.
    reversed: bool,
    init: RefCell<bool>,
}

impl ConcatIterator {
    /// Note: new ConcatIterator is invalid(Not pointer first element)
    pub fn new(tables: Vec<Table>, reversed: bool) -> ConcatIterator {
        let iters = tables
            .clone()
            .into_iter()
            .map(|tb| IteratorImpl::new(tb, reversed))
            .collect::<Vec<_>>();
        Self {
            index: RefCell::new(-1),
            iters,
            tables,
            reversed,
            init: RefCell::new(false),
        }
    }

    fn set_idx(&self, idx: isize) {
        if idx >= self.iters.len() as isize {
            *self.index.borrow_mut() = -1;
        } else {
            *self.index.borrow_mut() = idx;
        }
    }

    fn get_cur(&self) -> Option<&IteratorImpl> {
        #[cfg(test)]
        assert!(*self.index.borrow() >= 0, "it should be not happen");

        let cur = self.index.borrow();
        let iter = &self.iters[*cur as usize];
        Some(iter)
    }
}

impl Xiterator for ConcatIterator {
    type Output = IteratorItem;

    /// advances our concat iterator.
    fn next(&self) -> Option<Self::Output> {
        if self.iters.is_empty() {
            return None;
        }
        if !*self.init.borrow() {
            // Not init
            if !self.reversed {
                self.set_idx(0);
            } else {
                self.set_idx((self.iters.len() - 1) as isize);
            }
        }

        while (!self.reversed && *self.index.borrow() < self.iters.len() as isize)
            || (self.reversed && *self.index.borrow() >= 0)
        {
            let mut index = *self.index.borrow();
            let itr = self.get_cur().unwrap();
            if let Some(item) = itr.next() {
                return Some(item);
            }
            // Get next iterator
            if !self.reversed {
                index += 1;
            } else {
                index -= 1;
            }
            self.set_idx(index);
        }

        None
    }

    fn rewind(&self) -> Option<Self::Output> {
        if self.iters.is_empty() {
            return None;
        }
        // 1: reset iterator of tables
        if !self.reversed {
            self.set_idx(0);
        } else {
            self.set_idx(self.iters.len() as isize - 1);
        }
        // 2: reset iterator of current table
        self.get_cur().unwrap().rewind()
    }

    /// Brings us to element >= key if reversed is false. Otherwise, <= key.
    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        if self.iters.is_empty() {
            return None;
        }
        if !self.reversed {
            // >= key
            let idx = self.tables.binary_search_by(|tb| tb.biggest().cmp(key));
            if idx.is_ok() {
                self.set_idx(idx.unwrap() as isize);
                return self.get_cur().unwrap()._seek(key);
            }
            let idx = idx.err().unwrap();
            if idx < self.tables.len() {
                self.set_idx(idx as isize);
                return self.get_cur().unwrap()._seek(key);
            }
            // not found
            self.set_idx(-1);
            None
        } else {
            let idx = self.tables.binary_search_by(|tb| tb.smallest().cmp(key));
            if idx.is_ok() {
                self.set_idx(idx.unwrap() as isize);
                return self.get_cur().unwrap()._seek(key);
            }
            let idx = idx.err().unwrap();
            if idx == 0 {
                self.set_idx(-1);
                return None;
            }
            self.set_idx((idx - 1) as isize);
            self.get_cur().unwrap().seek_for_prev(key)
        }
    }

    fn peek(&self) -> Option<Self::Output> {
        let cur = self.get_cur();
        if cur.is_none() {
            return None;
        }
        cur.as_ref().unwrap().peek()
    }
}

impl fmt::Display for ConcatIterator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let cur = self
            .get_cur()
            .map(|iter| format!("{}", iter))
            .or_else(|| Some("None".to_string()))
            .unwrap();
        let table_str = self
            .tables
            .iter()
            .map(|t| format!("{}", t.to_ref()))
            .collect::<Vec<_>>()
            .join(",");
        f.write_fmt(format_args!(
            "index:{}, iters:{}, tables:{}, reversed:{}, cur:{}",
            *self.index.borrow(),
            self.iters.len(),
            table_str,
            self.reversed,
            cur
        ))
    }
}

#[test]
fn it() {
    let n = vec![2, 19, 30, 31, 33, 34, 35, 37];
    println!(
        "{:?}",
        n.binary_search_by(|a| {
            match a.cmp(&1) {
                Ordering::Equal => Ordering::Equal,
                Ordering::Greater => Ordering::Equal,
                Ordering::Less => Ordering::Less,
            }
        })
    );
}
