use std::sync::atomic::AtomicU8;

/// Specifies how data in LSM table files and value log files should
/// be loaded.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileLoadingMode {
    /// Indicates that files must be loaded using standard I/O
    FileIO,
    /// Indicates that files must be loaded into RAM
    LoadToRADM,
    /// Indicates that the file must be memory-mapped
    MemoryMap,
}

//
// #[test]
// fn it_atomic() {
//     let x = AtomicU8::new(0);
//     let y = AtomicU8::new(0);
//
//     let
// }
