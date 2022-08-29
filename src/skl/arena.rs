use crate::skl::small_allocate::{Allocate, Slice, SmallAllocate};
// use crate::skl::{Node, OwnedNode, MAX_HEIGHT, MAX_NODE_SIZE};
use crate::skl::Node;
use crate::y::ValueStruct;
use std::default;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::{addr_of, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::Duration;

const OFFSET_SIZE: usize = size_of::<u32>();
// FIXME: i don't know
const PTR_ALIGN: usize = 7;

/// `Arena` should be lock-free.
#[derive(Debug)]
pub struct Arena<T: Allocate> {
    n: AtomicU32,
    slice: T,
}

unsafe impl Send for Arena<SmallAllocate> {}

unsafe impl Sync for Arena<SmallAllocate> {}

impl Arena<SmallAllocate> {
    pub(crate) fn size(&self) -> u32 {
        self.n.load(Ordering::Acquire)
    }

    fn reset(&self) {
        self.n.store(0, Ordering::SeqCst)
    }

    // Returns a pointer to the node located at offset. If the offset is
    // zero, then the null node pointer is returned.
    pub(crate) fn get_node(&self, offset: usize) -> Option<&Node> {
        if offset == 0 {
            return None;
        }
        let node_sz = Node::size();
        let ptr = self.slice.borrow_slice(offset, node_sz).as_ptr();
        unsafe { Some(&*(*ptr as *const Node)) }
    }

    pub(crate) fn get_mut_node(&mut self, offset: usize) -> Option<&mut Node> {
        if offset == 0 {
            return None;
        }
        let node_sz = Node::size();
        let mut slice = self.slice.borrow_mut_slice(offset, node_sz);
        let ptr = slice.as_mut_ptr();
        unsafe { Some(&mut *(ptr as *mut Node)) }
    }

    // Returns start location
    pub(crate) fn put_key(&self, key: &[u8]) -> u32 {
        let start = self.n.fetch_add(key.len() as u32, Ordering::SeqCst) as usize;
        let end = start + key.len();
        let mut slice = self.slice.borrow_vec(start, end);
        let mut data = slice.get_mut_data();
        data.copy_from_slice(key);
        start as u32
    }

    // Put will *copy* val into arena. To make better use of this, reuse your input
    // val buffer. Returns an offset into buf. User is responsible for remembering
    // size of val. We could also store this size inside arena but the encoding and
    // decoding will incur some overhead.
    pub(crate) fn put_val(&self, v: &ValueStruct) -> (u32, usize) {
        let buf: Vec<u8> = v.to_vec();
        let offset = self.put_key(buf.as_slice());
        (offset, buf.len())
    }

    // Returns byte slice at offset.
    pub(crate) fn get_key(&self, offset: u32, size: u16) -> Slice {
        let slice = self.slice.borrow_vec(offset as usize, size as usize);
        slice
    }

    // Returns byte slice at offset. The given size should be just the value
    // size and should NOT include the meta bytes.
    pub(crate) fn get_val(&self, offset: u32, size: u16) -> ValueStruct {
        let slice = self.slice.borrow_vec(offset as usize, size as usize);
        let value = ValueStruct::from(slice);
        value
    }

    // FIXME:
    pub(crate) fn put_node(&self, height: isize) -> u32 {
        let offset: usize = Node::size();
        // *Note*: the memory set 0 value.
        let key = vec![0u8; offset];
        self.put_key(&key)
    }

    // Returns the offset of `node` in the arena. If the `node` pointer is
    // nil, then the zero offset is returned.
    pub(crate) fn get_node_offset(&self, node: *const Node) -> usize {
        if node.is_null() {
            return 0;
        }
        let node = node as *const u8;
        unsafe { node.offset_from(self.slice.get_data_ptr()) as usize }
    }
}
