use crate::skl::BlockBytes;
use crate::skl::Chunk;
use crate::table::iterator::{IteratorImpl, IteratorItem};
use crate::y::{CAS_SIZE, META_SIZE, USER_META_SIZE, VALUE_SIZE};
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde_json::Value;
use std::borrow::Borrow;
use std::cell::{RefCell, RefMut};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt;
use std::fmt::Formatter;
use std::io::{Cursor, Read, Write};
use std::iter::Iterator as stdIterator;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};

/// ValueStruct represents the value info that can be associated with a key, but also the internal
/// Meta field.
/// |meta|user_meta|cas_counter|value_size|value|
#[derive(Debug, Default, PartialEq)]
#[repr(C)]
pub struct ValueStruct {
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) cas_counter: u64,
    pub(crate) value: Vec<u8>,
}

impl ValueStruct {
    pub(crate) fn new(value: Vec<u8>, meta: u8, user_meta: u8, cas_counter: u64) -> ValueStruct {
        ValueStruct {
            meta,
            user_meta,
            cas_counter,
            value,
        }
    }
    pub(crate) const fn header_size() -> usize {
        10
    }

    fn size(&self) -> usize {
        Self::header_size() + self.value.len()
    }

    pub(crate) fn write_data(&self, mut buffer: &mut [u8]) {
        use std::io::Write;
        let mut cursor = Cursor::new(buffer);
        cursor.write_u8(self.meta).unwrap();
        cursor.write_u8(self.user_meta).unwrap();
        cursor.write_u64::<BigEndian>(self.cas_counter).unwrap();
        cursor.write_all(&self.value).unwrap();
    }

    pub(crate) fn read_data(&mut self, buffer: &[u8]) {
        use std::io::Read;
        let mut cursor = Cursor::new(buffer);
        self.meta = cursor.read_u8().unwrap();
        self.user_meta = cursor.read_u8().unwrap();
        self.cas_counter = cursor.read_u64::<BigEndian>().unwrap();
        self.value.extend_from_slice(&buffer[Self::header_size()..]);
    }
}

impl ValueStruct {
    #[inline]
    pub(crate) fn get_data_mut_ptr(&self) -> *mut u8 {
        self as *const Self as *const u8 as *mut u8
    }
}

impl<T> From<T> for ValueStruct
where
    T: AsRef<[u8]>,
{
    fn from(buffer: T) -> Self {
        let mut v = ValueStruct::default();
        v.read_data(buffer.as_ref());
        v
    }
}

impl Into<Vec<u8>> for &ValueStruct {
    fn into(self) -> Vec<u8> {
        let mut buffer = vec![0; self.size()];
        self.write_data(&mut buffer);
        buffer
    }
}

pub trait Xiterator {
    type Output;
    fn next(&self) -> Option<Self::Output>;
    fn rewind(&self) -> Option<Self::Output>;
    fn seek(&self, key: &[u8]) -> Option<Self::Output>;
    fn peek(&self) -> Option<Self::Output> {
        todo!()
    }
}

pub trait KeyValue<V> {
    fn key(&self) -> &[u8];
    fn value(&self) -> V;
}

pub struct MergeIterator<'a> {
    cur_key: RefCell<Vec<u8>>,
    reverse: bool,
    all: Vec<IteratorImpl<'a>>,
    elements: RefCell<Vec<MergeIteratorElement<IteratorImpl<'a>>>>,
}

impl<'a> MergeIterator<'a> {
    pub fn new(iters: Vec<IteratorImpl<'a>>, reverse: bool) -> MergeIterator<'a> {
        let m = MergeIterator {
            cur_key: RefCell::new(vec![]),
            reverse,
            all: iters,
            elements: RefCell::default(),
        };
        m.init();
        m
    }

    // initHeap checks all iterators and initializes our heap and array of keys.
    // Whenever we reverse direction, we need to run this.
    // If use 'a can not compiled, Haha
    fn init(&self) {
        self.elements.borrow_mut().clear();
        for (idx, iter) in self.all.iter().enumerate() {
            if iter.peek().is_none() {
                continue;
            }
            self.elements.borrow_mut().push(MergeIteratorElement {
                nice: idx as isize,
                itr: iter,
                reverse: self.reverse,
            });
        }
        self.elements.borrow_mut().sort();
        if let Some(cur_itr) = self.elements.borrow().first() {
            let cur_key = cur_itr.get_itr().peek();
            self.store_key(cur_key.as_ref().unwrap().key());
        }
    }

    fn store_key(&self, key: &[u8]) {
        self.cur_key.borrow_mut().clear();
        self.cur_key.borrow_mut().extend_from_slice(key);
    }
}

impl<'remainder> Xiterator for MergeIterator<'remainder> {
    type Output = IteratorItem;

    fn next(&self) -> Option<Self::Output> {
        if self.elements.borrow().is_empty() {
            return None;
        }

        let mut keyvalue: Option<IteratorItem> = None;
        for (idx, tb_iter) in self.elements.borrow().iter().enumerate() {
            if idx == 0 {
                if let Some(item) = tb_iter.get_itr().next() {
                    keyvalue = Some(item);
                }
            } else {
                if let Some(item) = tb_iter.get_itr().next() {
                    if item.key() == keyvalue.as_ref().unwrap().key() {
                        continue;
                    } else {
                        // Because has moved it pointer
                        tb_iter.get_itr().prev();
                    }
                }
            }
        }
        self.init();
        if let Some(ref item) = keyvalue {
            self.store_key(item.key());
        }

        keyvalue
    }

    fn rewind(&self) -> Option<Self::Output> {
        for itr in self.all.iter() {
            itr.rewind();
        }
        self.init();
        self.peek()
    }

    fn seek(&self, key: &[u8]) -> Option<Self::Output> {
        for iter in self.all.iter() {
            iter.seek(key);
        }
        self.init();
        self.peek()
    }

    fn peek(&self) -> Option<Self::Output> {
        if let Some(itr) = self.elements.borrow().first() {
            return itr.get_itr().peek();
        }
        None
    }
}

#[derive(Debug)]
pub struct MergeIteratorElement<I>
where
    I: Xiterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    nice: isize,
    itr: *const I, // todo maybe use Rc<RefCell> advoid pointer (self reference, lifetime, trait lifetime)
    reverse: bool,
}

impl<I> fmt::Display for MergeIteratorElement<I>
where
    I: Xiterator<Output = IteratorItem> + KeyValue<ValueStruct> + fmt::Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        unsafe {
            write!(
                f,
                "nice:{}, reverse: {}, key: {}",
                self.nice,
                self.reverse,
                self.itr.as_ref().unwrap()
            )
        }
    }
}

impl<I> MergeIteratorElement<I>
where
    I: Xiterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    fn get_itr(&self) -> &I {
        unsafe { &*self.itr }
    }
}

impl<I> PartialEq<Self> for MergeIteratorElement<I>
where
    I: Xiterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    fn eq(&self, other: &Self) -> bool {
        self.get_itr().key() == other.get_itr().key()
    }
}

impl<I> PartialOrd<Self> for MergeIteratorElement<I>
where
    I: Xiterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.get_itr().key() == other.get_itr().key() {
            return Some(self.nice.cmp(&other.nice));
        }
        Some(self.get_itr().key().cmp(other.get_itr().key()))
    }
}

impl<I> Eq for MergeIteratorElement<I> where
    I: Xiterator<Output = IteratorItem> + KeyValue<ValueStruct>
{
}

impl<I> Ord for MergeIteratorElement<I>
where
    I: Xiterator<Output = IteratorItem> + KeyValue<ValueStruct>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.nice.cmp(&other.nice)
    }
}

#[test]
fn merge_iter_element() {
    #[derive(Debug)]
    struct TestIterator {
        key: Vec<u8>,
    }

    impl fmt::Display for TestIterator {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "key: {}", String::from_utf8_lossy(&self.key))
        }
    }

    impl super::iterator::Xiterator for TestIterator {
        type Output = IteratorItem;

        fn next(&self) -> Option<Self::Output> {
            todo!()
        }

        fn rewind(&self) -> Option<Self::Output> {
            todo!()
        }

        fn seek(&self, key: &[u8]) -> Option<Self::Output> {
            todo!()
        }
    }

    impl KeyValue<ValueStruct> for TestIterator {
        fn key(&self) -> &[u8] {
            &self.key
        }

        fn value(&self) -> ValueStruct {
            todo!()
        }
    }

    let t1 = &TestIterator {
        key: b"abd".to_vec(),
    };
    let t2 = &TestIterator {
        key: b"abc".to_vec(),
    };
    let t3 = &TestIterator {
        key: b"abc".to_vec(),
    };
    let t4 = &TestIterator {
        key: b"abc".to_vec(),
    };
    let e1 = MergeIteratorElement {
        nice: 1,
        itr: t1 as *const TestIterator,
        reverse: false,
    };

    let e2 = MergeIteratorElement {
        nice: 1,
        itr: t2 as *const TestIterator,
        reverse: false,
    };
    let e3 = MergeIteratorElement {
        nice: 2,
        itr: t3 as *const TestIterator,
        reverse: false,
    };
    let e4 = MergeIteratorElement {
        nice: 2,
        itr: t4 as *const TestIterator,
        reverse: false,
    };
    //
    let mut e = vec![e4, e1, e2, e3];
    e.sort();
    e.iter().for_each(|e| {
        println!("{}", e);
    });
}
