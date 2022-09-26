/// Specifies how data in LSM table files and value log files should
/// be loaded.
pub enum FileLoadingMode {
    /// Indicates that files must be loaded using standard I/O
    FileIO,
    /// Indicates that files must be loaded into RAM
    LoadToRADM,
    /// Indicates that the file must be memory-mapped
    MemoryMap,
}
