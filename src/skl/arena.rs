use crate::skl::{Node, OwnedNode, MAX_HEIGHT, MAX_NODE_SIZE};
use crate::y::ValueStruct;
use std::default;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::addr_of;
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::spawn;

const OFFSET_SIZE: usize = size_of::<u32>();
// FIXME: i don't know
const PTR_ALIGN: usize = 7;

/// `Arena` should be lock-free.
#[derive(Debug)]
pub struct Arena<'a> {
    n: AtomicU32,
    slice: &'a mut ArenaSlice,
}

impl<'a> Arena<'a> {
    fn new(buffer: &mut [u8]) -> Arena {
        let len = buffer.len();
        let mut arena = Arena {
            n: AtomicU32::new(1),
            slice: ArenaSlice::from_slice_mut(buffer),
        };
        arena.slice.len = (len - size_of::<u32>()) as u32;
        arena
    }

    fn size(&self) -> u32 {
        self.n.load(Ordering::Acquire)
    }

    fn reset(&mut self) {
        self.n.store(0, Ordering::SeqCst)
    }
    // allocates a node in the arena. The node is aligned on a pointer-sized
    // boundary. The arena offset of the node is returned.
    fn put_node(&mut self, height: usize) -> u32 {
        // Compute the amount of the tower that will never be used, since the height
        // is less than MAX_HEIGHT
        // let unused_size = (MAX_HEIGHT - height) * OFFSET_SIZE;
        // // Pad the allocation with enough butes to ensure pointer alignment.
        // let l = (MAX_NODE_SIZE - unused_size + PTR_ALIGN);
        // let n = self.n.fetch_add(l as u32, Ordering::Acquire);
        todo!()
    }

    // Returns a pointer to the node located at offset. If the offset is
    // zero, then the null node pointer is returned.
    fn get_node(&self, offset: usize) -> Option<&Node> {
        if offset == 0 {
            return None;
        }
        let node_sz = Node::size();
        let ptr = &self.slice.get_data_slice()[offset..offset + node_sz].as_ptr();
        unsafe { Some(&*(*ptr as *const Node)) }
    }

    // getNodeOffset returns the offset of node in the arena. If the node pointer is
    // nil, then the zero offset is returned.
    fn get_node_offset(&self, node: &Node) -> u32 {
        todo!()
    }

    // Put will *copy* val into arena. To make better use of this, reuse your input
    // val buffer. Returns an offset into buf. User is responsible for remembering
    // size of val. We could also store this size inside arena but the encoding and
    // decoding will incur some overhead.
    pub(crate) fn put_value(&mut self, mut value: ValueStruct) -> u32 {
        let encode_size = value.encode_size();
        let start = self.n.fetch_add(encode_size as u32, Ordering::SeqCst) as usize;
        let end = start + encode_size;
        let mut slice = self.slice.get_data_slice_mut();
        value.encode(&mut slice[start..end]);
        start as u32
    }

    // Returns start location
    fn put_key(&mut self, key: &[u8]) -> u32 {
        let start = self.n.fetch_add(key.len() as u32, Ordering::SeqCst) as usize;
        let end = start + key.len();
        let mut slice = self.slice.get_data_slice_mut();
        slice[start..end].copy_from_slice(key);
        start as u32
    }

    // returns byte slice at offset.
    pub(crate) fn get_key(&self, offset: u32, size: u16) -> &[u8] {
        let slice = self.slice.get_data_slice();
        let offset = offset as usize;
        return &slice[offset..(offset + size as usize)];
    }

    // getVal returns byte slice at offset. The given size should be just the value
    // size and should NOT include the meta bytes.
    fn get_val(&mut self, offset: u32, size: u16) -> ValueStruct {
        let offset = offset as usize;
        let mut slice = self.slice.get_data_slice_mut();
        let mut yal = ValueStruct::from(
            &slice[offset..(offset + ValueStruct::value_struct_serialized_size(size))],
        );
        yal
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct ArenaSlice {
    len: u32,
    ptr: PhantomData<u8>,
}

impl ArenaSlice {
    #[inline]
    pub(crate) fn get_data_mut_ptr(&mut self) -> *mut u8 {
        &mut self.ptr as *mut PhantomData<u8> as *mut u8
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        &self.ptr as *const PhantomData<u8> as *const u8
    }

    #[inline]
    pub(crate) fn get_data_slice(&self) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { from_raw_parts(ptr, self.byte_size()) }
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { from_raw_parts(ptr, self.byte_size()) }
    }

    #[inline]
    pub(crate) fn get_data_slice_mut(&mut self) -> &mut [u8] {
        let ptr = self.get_data_mut_ptr();

        unsafe { from_raw_parts_mut(ptr, self.byte_size()) }
    }

    #[inline]
    pub(crate) fn from_slice(buffer: &[u8]) -> &Self {
        unsafe { &*(buffer.as_ptr() as *const ArenaSlice) }
    }

    #[inline]
    pub(crate) fn from_slice_mut(mut buffer: &mut [u8]) -> &mut Self {
        unsafe { &mut *(buffer.as_mut_ptr() as *mut ArenaSlice) }
    }

    #[inline]
    pub(crate) fn byte_size(&self) -> usize {
        self.len as usize
    }
}

#[test]
fn test_value() {}

#[test]
fn test_arena() {
    let mut buffer = vec![0u8; 1024];
    let mut arena = Arena::new(&mut buffer);
    let key = &vec![12u8; 10];
    let start = arena.put_key(key);
    let got_key = arena.get_key(start, 10);
    assert_eq!(got_key, key);

    let v = b"hello";
    let value = ValueStruct::new(v, 10, 10, 10);
    let end = value.encode_size() as u16;
    let start = arena.put_value(value);
    let got_value = arena.get_val(start, end);
    println!("value: {:?}", got_value);
}
