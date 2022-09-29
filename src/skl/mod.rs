mod alloc;
mod arena;
mod cursor;

mod node;
mod skip;
pub mod small_allocate;
use crate::skl::node::Node;
use crate::y::ValueStruct;
use crate::{must_align, BadgerErr};
pub use alloc::{Allocate, BlockBytes, Chunk, SmartAllocate};
pub use arena::Arena;
pub use cursor::Cursor;
use std::mem::size_of;

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;
