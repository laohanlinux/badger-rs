use crate::table::builder::Header;
use crate::table::table::Table;
use crate::y::{Result, ValueStruct};
use crate::Error;
use std::ops::Deref;
use std::{cmp, io};

#[derive(Debug)]
pub(crate) struct BlockIterator {
    data: Vec<u8>,
    pos: u32,
    err: Result<()>,
    base_key: Vec<u8>,
    key: Vec<u8>,
    val: Vec<u8>,
    init: bool,
    last: Header,
}

impl Default for BlockIterator {
    fn default() -> Self {
        Self {
            data: vec![],
            pos: 0,
            err: Ok(()),
            base_key: vec![],
            key: vec![],
            val: vec![],
            init: false,
            last: Default::default(),
        }
    }
}

pub(crate) enum BlockIteratorSeek {
    Origin,
    Current,
}

impl BlockIterator {
    pub(crate) fn Seek(&mut self, key: &[u8], whence: BlockIteratorSeek) {
        self.err = Ok(());
        match whence {
            BlockIteratorSeek::Origin => self.reset(),
            BlockIteratorSeek::Current => {}
        }

        let mut done = false;
        self.init();
        while self.valid() {}
    }

    pub(crate) fn next(&mut self) {
        self.init = true;
        self.err = Ok(());
        if self.pos >= self.data.len() as u32 {
            self.err = Err(Error::Io(io::ErrorKind::UnexpectedEof.to_string()));
            return;
        }
    }

    fn reset(&mut self) {
        *self = Default::default();
    }

    fn init(&mut self) {
        if !self.init {
            self.next();
        }
    }

    fn valid(&self) -> bool {
        self.err.is_ok()
    }

    fn error(&self) -> &Result<()> {
        &self.err
    }

    fn close(&self) {}
}

struct Iterator<'a> {
    table: &'a Table,
    bpos: usize,
    bi: &'a BlockIterator,
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

// An iterator for a Table.
// #[doc(hidden)]
// struct Iterator<'a> {
//     table: &'a Table,
//     bpos: usize,
// }

#[test]
fn it() {}
