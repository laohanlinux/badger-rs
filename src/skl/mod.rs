mod alloc;
mod arena;
mod cursor;
mod node;
mod skip;

pub use alloc::*;
pub use arena::Arena;
pub use cursor::Cursor;
pub use node::Node;
pub use skip::*;

const MAX_HEIGHT: usize = 20;
const HEIGHT_INCREASE: u32 = u32::MAX / 3;
