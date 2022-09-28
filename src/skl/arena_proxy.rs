use crate::skl::small_allocate::SmallAllocate;
use crate::skl::{Allocate, Chunk, Node, SmartAllocate};
use crate::y::ValueStruct;
use std::alloc::Layout;
use std::mem::size_of;

// use crate::skl::{Arena, SmartAllocate};
//
pub enum ArenaLayout {
    Slice,
    Node,
}
//
// pub struct ArenaProxy {
//     slice_arena: Arena<SmartAllocate>,
//     node_arena: Arena<SmartAllocate>,
// }
//
//
// impl ArenaProxy {
//     fn new(slice_arena_size: usize, node_arena_size: usize) -> Self {
//         Self {
//             slice_arena: Arena::new(slice_arena_size),
//             node_arena: Arena::new(node_arena_size),
//         }
//     }
// }

trait Arena: Send + Sync {
    fn size(layout: ArenaLayout) -> usize {
        match layout {
            ArenaLayout::Slice => size_of::<u8>(),
            ArenaLayout::Node => size_of::<Node>(),
        }
    }
    fn reset(&self);
    fn valid(&self, layout: ArenaLayout) -> bool;
    fn get_node(&self, offset: usize) -> &Node;
    fn get_mut_node(&mut self, offset: usize) -> &mut Node;
    fn put_key(&self, key: &[u8]) -> u32;
    fn get_key<C: Chunk>(&self, offset: u32, size: u16) -> C;
    fn put_value(&self, value: &ValueStruct) -> (u32, u16);
    fn get_val(&self, offset: u32, size: u16) -> ValueStruct;
    fn put_node(&self, height: isize) -> u32;
    fn get_node_offset(&self, node: *const Node) -> usize;
}

pub struct SmartArena<A: Allocate> {
    slice_arena: A,
    node_slice: A,
}

// impl Arena for SmartArena<SmallAllocate> {
//     fn reset(&self) {}
//
//     fn valid(&self, layout: ArenaLayout) -> bool {
//         todo!()
//     }
//
//     fn get_node(&self, offset: usize) -> &Node {
//         todo!()
//     }
//
//     fn get_mut_node(&mut self, offset: usize) -> &mut Node {
//         todo!()
//     }
//
//     fn put_key(&self, key: &[u8]) -> u32 {
//         todo!()
//     }
//
//     fn get_key<C: Chunk>(&self, offset: u32, size: u16) -> C {
//         todo!()
//     }
//
//     fn put_value(&self, value: &ValueStruct) -> (u32, u16) {
//         todo!()
//     }
//
//     fn get_val(&self, offset: u32, size: u16) -> ValueStruct {
//         todo!()
//     }
//
//     fn put_node(&self, height: isize) -> u32 {
//         todo!()
//     }
//
//     fn get_node_offset(&self, node: *const Node) -> usize {
//         todo!()
//     }
// }

#[test]
fn t() {}
