use crate::skl::{Node, OwnedNode};
use crate::y::ValueStruct;
use std::default;
use std::ptr::addr_of;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// `Arena` should be lock-free.
pub struct Arena {
    n: AtomicU32,
    buf: Vec<u8>,
}

impl Arena {
    fn new(n: u32) -> Arena {
        let buf = vec![0u8; n as usize];
        Arena {
            n: AtomicU32::new(n),
            buf,
        }
    }

    fn size(&self) -> u32 {
        self.n.load(Ordering::Acquire)
    }

    fn reset(&mut self) {
        self.n.store(0, Ordering::SeqCst)
    }

    // putNode allocates a node in the arena. The node is aligned on a pointer-sized
    // boundary. The arena offset of the node is returned.
    fn put_node(&mut self, height: usize) -> u32 {
        todo!()
    }

    // Returns a pointer to the node located at offset. If the offset is
    // zero, then the null node pointer is returned.
    fn get_node(&self, offset: usize) -> Option<Node> {
        if offset == 0 {
            return None;
        }
        None
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
        let n = self.n.fetch_add(encode_size as u32, Ordering::SeqCst);
        let m = n - encode_size as u32;
        value.encode(&mut self.buf[m as usize..]);
        m
    }

    // Returns start location
    fn put_key(&mut self, key: &[u8]) -> u32 {
        let n = self.n.fetch_add(key.len() as u32, Ordering::SeqCst);
        assert!(
            n <= self.buf.len() as u32,
            "arena too small, toWrite: {} newTotal: {} limit: {}",
            key.len(),
            n,
            self.buf.len()
        );
        let m = n as usize - key.len();
        self.buf[m..n as usize].copy_from_slice(key);
        m as u32
    }

    // returns byte slice at offset.
    pub(crate) fn get_key(&self, offset: u32, size: u16) -> &[u8] {
        let offset = offset as usize;
        return &self.buf[offset..(offset + size as usize)];
    }

    // getVal returns byte slice at offset. The given size should be just the value
    // size and should NOT include the meta bytes.
    fn get_val(&self, offset: u32, size: u16) -> ValueStruct {
        let offset = offset as usize;
        let mut yal = ValueStruct::from(
            &self.buf[offset..(offset + ValueStruct::value_struct_serialized_size(size))],
        );
        yal
    }
}

#[test]
fn test_arena() {}
