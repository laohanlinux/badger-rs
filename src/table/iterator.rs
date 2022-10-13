use crate::table::builder::Header;
use crate::table::table::{Block, Table, TableCore};
use crate::y::{Result, ValueStruct};
use crate::Error;
use std::cell::{Ref, RefCell, RefMut};
use std::ops::Deref;
use std::{cmp, io};

#[derive(Debug)]
pub(crate) struct BlockIterator {
    data: Vec<u8>,
    pos: RefCell<u32>,
    base_key: RefCell<Vec<u8>>,
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

impl BlockIterator {
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            pos: RefCell::new(0),
            base_key: RefCell::new(vec![]),
            last_header: RefCell::new(Header::default()),
        }
    }

    /// Brings us to the first block element that is >= input key.
    pub fn seek(&self, key: &[u8], whence: BlockIteratorSeek) -> Option<BlockIteratorItem> {
        match whence {
            BlockIteratorSeek::Origin => self.reset(),
            BlockIteratorSeek::Current => {}
        }
        while let Some(item) = self.next() {
            if item.key() >= key {
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

    fn prev(&self) -> Option<BlockIteratorItem> {
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
        self.base_key.borrow_mut().clear();
    }

    pub(crate) fn next(&self) -> Option<BlockIteratorItem> {
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
            self.base_key
                .borrow_mut()
                .extend_from_slice(&self.data[*pos as usize..(*pos + h.k_len as u32) as usize]);
        }
        drop(pos);
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

// impl<'a> std::iter::Iterator for BlockIterator<'a> {
//     type Item = BlockIteratorItem<'a>;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         self._next()
//     }
// }

struct Iterator<'a> {
    table: &'a TableCore,
    bpos: RefCell<isize>,
    bi: RefCell<Option<BlockIterator>>,
    reversed: RefCell<bool>,
}

impl<'a> Iterator<'a> {
    fn reset(&self) {
        *self.bpos.borrow_mut() = 0;
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

    fn seek_to_first(&self) -> Option<IteratorItem> {
        if self.table.block_index.is_empty() {
            return None;
        }
        *self.bpos.borrow_mut() = 0;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow() as usize);
        bi.as_ref()
            .unwrap()
            .seek_to_first()
            .map(|b_item| b_item.into())
    }

    fn seek_to_last(&self) -> Option<IteratorItem> {
        if self.table.block_index.is_empty() {
            return None;
        }
        *self.bpos.borrow_mut() = (self.table.block_index.len() - 1) as isize;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow() as usize);
        bi.as_ref()
            .unwrap()
            .seek_to_last()
            .map(|b_item| b_item.into())
    }

    fn seek_helper(&self, block_index: usize, key: &[u8]) -> Option<IteratorItem> {
        *self.bpos.borrow_mut() = block_index as isize;
        let bi = self.get_bi_by_bpos(*self.bpos.borrow() as usize);
        bi.as_ref()
            .unwrap()
            .seek(key, BlockIteratorSeek::Origin)
            .map(|b_item| b_item.into())
    }

    // Brings us to a key that is >= input key.
    fn seek_from(&self, key: &[u8], whence: BlockIteratorSeek) -> Option<IteratorItem> {
        match whence {
            BlockIteratorSeek::Origin => self.reset(),
            BlockIteratorSeek::Current => {}
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

    // brings us to a key that is >= input key.
    fn seek(&self, key: &[u8]) -> Option<IteratorItem> {
        self.seek_from(key, BlockIteratorSeek::Origin)
    }

    // will reset iterator and seek to <= key.
    fn seek_for_prev(&self, key: &[u8]) -> Option<IteratorItem> {
        // TODO: Optimize this. We shouldn't have to take a Prev step.
        if let Some(item) = self.seek_from(key, BlockIteratorSeek::Origin) {
            if item.key == key {
                return Some(item);
            }
            return self.prev(key);
        }
        None
    }

    fn prev(&self, key: &[u8]) -> Option<IteratorItem> {
        if *self.bpos.borrow() < 0 {
            // maybe reset bpos ==0?
            return None;
        }
        let bi = self.get_or_set_bi(*self.bpos.borrow() as usize);
        let item = bi.as_ref().unwrap().prev();
        let item = item.map(|b_item| b_item.into());
        if item.is_some() {
            return item;
        }

        if *self.bpos.borrow() == 0 {
            return None;
        }
        {
            *self.bpos.borrow_mut() -= 1;
        }
        self.prev(key)
    }

    // fn recursion_pre(bpos: isize, table: &TableCore, key: &[u8]) -> Option<BlockIteratorItem> {
    //     if bpos < 0 {
    //         return None;
    //     }
    //     let block = table.block(bpos as usize).unwrap();
    //     let bi = BlockIterator::new(block.data);
    //     bi.seek(key)
    // }

    fn next(&self) -> Option<IteratorItem> {
        let bpos = self.bpos.borrow_mut();
        if *bpos >= self.table.block_index.len() as isize {
            return None;
        }
        let bi = self.get_or_set_bi(*bpos as usize);
        bi.as_ref().unwrap().next().map(|b_item| b_item.into())
    }

    fn rewind(&self) {
        todo!()
    }
}

pub(crate) struct IteratorItem {
    key: Vec<u8>,
    value: ValueStruct,
}

impl<'a> From<BlockIteratorItem<'a>> for IteratorItem {
    fn from(b_item: BlockIteratorItem) -> Self {
        let key = b_item.key;
        let value = ValueStruct::from(b_item.value);
        IteratorItem { key, value }
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
fn it() {
    let n = vec![2, 19, 30, 31, 33, 34, 35, 37];
    println!("{:?}", n.binary_search_by(|a| a.cmp(&1)));
}
