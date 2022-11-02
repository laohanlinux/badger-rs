use std::fs::File;

struct LogFile {
    _path: Box<String>,
    fd: File,
    fid: u32,
    _mmap: Option<Mmap>,
    sz: u32,
}

struct ValueLogCore {}
