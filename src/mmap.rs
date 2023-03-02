use crate::Error::Unexpected;
use crate::Result;
use fmmap::raw::tokio::AsyncDiskMmapFile;
use fmmap::tokio::{AsyncMmapFile, AsyncMmapFileExt, AsyncOptions};
use memmap::MmapMut;
use tokio::fs::File;

pub(crate) struct AsyncLogFile {
    path: Box<String>,
    sz: usize,
    fp: Option<tokio::fs::File>,
    m:  Option<MmapMut>,
}

// impl AsyncMMAP {
//     async fn new(path: &str) -> Result<Self> {
//         let fp = std::fs::File::options().read(true).open(path)?;
//         let sz = fp.metadata()?.len();
//         Ok(AsyncMMAP {
//             fp: None,
//             m: Some(m),
//         })
//     }
//
//     pub(crate) fn mut_fp(&mut self) -> &mut File {
//         self.fp.as_mut().unwrap()
//     }
//
//     pub(crate) fn fp(&self) -> &File {
//         self.fp.as_ref().unwrap()
//     }
//
//     pub(crate) async fn set_len(&self, offset: u64)  -> Result<()>{
//         if let Some(m) = &self.m {
//             m.flush()?;
//         }
//         self.fp().set_len(offset)?;
//         Ok(())
//     }
// }
//
// impl Drop for AsyncMMAP {
//     fn drop(&mut self) {
//         if let Some(m) = self.m.take() {}
//     }
// }

// #[tokio::test]
// async fn it() {
//     let mut m = AsyncMMAP::new("LICENSE").await;
//     assert!(m.is_ok());
//     println!("{:?}", m.as_ref().unwrap().m.as_ref().unwrap().as_slice());
//     let m = m.as_mut().unwrap().m.as_mut().unwrap();
//     let mut buffer = vec![0u8; 100];
//     m.read_exact(&mut buffer, 0).await;
// }
