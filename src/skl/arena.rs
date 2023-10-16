use crate::skl::node::Node;
use crate::y::ValueStruct;
use crate::{Allocate, SmallAlloc};
use std::mem::size_of;
use std::ptr::{slice_from_raw_parts, slice_from_raw_parts_mut, NonNull};


/// How to cals SkipList allocate size
/// 8(zero-bit) + key + value + node*N

/// `Arena` should be lock-free.
#[derive(Debug)]
pub struct Arena {
    alloc: SmallAlloc,
}

impl Arena {
    pub(crate) fn new(n: usize) -> Self {
        assert!(n > 0);
        // Don't store data at position 0 in order to reverse offset = 0 as a kind
        // of nil pointer
        Self {
            alloc: SmallAlloc::new(n),
        }
    }

    pub(crate) fn size(&self) -> u32 {
        self.alloc.len() as u32
    }

    pub(crate) fn cap(&self) -> u32 {
        self.alloc.cap() as u32
    }

    // TODO: maybe use MaybeUint instead
    // pub(crate) fn reset(&self) {
    //     self.alloc.reset();
    // }

    pub(crate) fn valid(&self) -> bool {
        // !self.slice.ptr.is_empty()
        todo!()
    }

    // Returns a pointer to the node located at offset. If the offset is
    // zero, then the null node pointer is returned.
    pub(crate) fn get_node(&self, offset: usize) -> Option<&Node> {
        if offset == 0 {
            return None;
        }
        unsafe { self.alloc.get_mut::<Node>(offset).as_ref() }
    }

    pub(crate) fn get_mut_node(&self, offset: usize) -> Option<&mut Node> {
        if offset == 0 {
            return None;
        }
        unsafe { self.alloc.get_mut::<Node>(offset).as_mut() }
    }

    // Returns start location
    pub(crate) fn put_key(&self, key: &[u8]) -> u32 {
        let offset = self.alloc.alloc(key.len());
        let buffer = unsafe { self.alloc.get_mut::<u8>(offset) };
        let mut buffer = unsafe { &mut *slice_from_raw_parts_mut(buffer, key.len()) };
        buffer.copy_from_slice(key);
        offset as u32
    }

    // Put will *copy* val into arena. To make better use of this, reuse your input
    // val buffer. Returns an offset into buf. User is responsible for remembering
    // size of val. We could also store this size inside arena but the encoding and
    // decoding will incur some overhead.
    pub(crate) fn put_val(&self, v: &ValueStruct) -> (u32, u16) {
        let buf: Vec<u8> = v.into();
        let offset = self.put_key(buf.as_slice());
        (offset, buf.len() as u16)
    }

    // Returns byte slice at offset.
    pub(crate) fn get_key(&self, offset: u32, size: u16) -> &[u8] {
        let buffer = unsafe { self.alloc.get_mut::<u8>(offset as usize) };
        unsafe { &*slice_from_raw_parts(buffer, size as usize) }
    }

    // Returns byte slice at offset. The given size should be just the value
    // size and should NOT include the meta bytes.
    pub(crate) fn get_val(&self, offset: u32, size: u16) -> ValueStruct {
        let buffer = self.get_key(offset, size);
        ValueStruct::from(buffer)
    }

    // Return byte slice at offset.
    // FIXME:
    pub(crate) fn put_node(&self, _height: isize) -> u32 {
        let offset = self.alloc.alloc(Node::align_size());
        offset as u32
    }

    // Returns the offset of `node` in the arena. If the `node` pointer is
    // nil, then the zero offset is returned.
    pub(crate) fn get_node_offset(&self, node: *const Node) -> usize {
        if node.is_null() {
            return 0;
        }

        let offset = unsafe { self.alloc.offset(node) };
        offset as usize
    }

    pub(crate) fn copy(&self) -> NonNull<Self> {
        let ptr = self as *const Self as *mut Self;
        NonNull::new(ptr).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Arena, Node, ValueStruct};
    use std::sync::Arc;
    use std::thread::spawn;

    #[test]
    fn t_arena_key() {
        let arena = Arena::new(1 << 20);
        let keys = vec![vec![1, 2, 3], vec![4, 5, 6, 7, 90]];
        let mut got = vec![];
        for key in keys.iter() {
            got.push(arena.put_key(key));
        }
        for (i, offset) in got.iter().enumerate() {
            let key = arena.get_key(*offset, keys[i].len() as u16);
            assert_eq!(key, keys[i]);
        }
    }

    #[test]
    fn t_arena_value() {
        let arena = Arena::new(1 << 20);
        let v = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let value = ValueStruct {
            meta: 1,
            user_meta: 1,
            cas_counter: 2,
            value: v,
        };
        let (start, n) = arena.put_val(&value);
        let load_value = arena.get_val(start, n);
        assert_eq!(value, load_value);
    }

    #[test]
    fn t_arena_store_node() {
        let arena = Arena::new(1 << 20);
        let mut starts = vec![];
        for i in 0..5 {
            let start = arena.put_node(i);
            let node = arena.get_mut_node(start as usize).unwrap();
            node.height = i as u16;
            node.value.fetch_add(i as u64, atomic::Ordering::Relaxed);
            starts.push((i, start));
        }

        for (i, start) in starts {
            let node = arena.get_mut_node(start as usize).unwrap();
            let value = node.value.load(atomic::Ordering::Relaxed);
            assert_eq!(node.height, i as u16);
            assert_eq!(value, i as u64);
        }

        let second_node = arena.get_node(Node::align_size()).unwrap();
        let offset = arena.get_node_offset(second_node);
        assert_eq!(offset, Node::align_size());
    }

    #[test]
    fn t_arena_currency() {
        let arena = Arc::new(Arena::new(1 << 20));
        let mut waits = vec![];
        for _i in 0..100 {
            let arena = Arc::clone(&arena);
            waits.push(spawn(move || arena.put_key(b"abc")));
        }

        let mut offsets = waits
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>();
        offsets.sort();
        println!("offsets: {:?}", offsets);
    }
}
