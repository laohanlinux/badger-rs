use std::cell::SyncUnsafeCell;
use std::mem::ManuallyDrop;
use std::thread::spawn;

#[derive(Clone)]
pub struct SafeVec {
    v: ManuallyDrop<Vec<u8>>,
}

unsafe impl Send for SafeVec {}

unsafe impl Sync for SafeVec {}

impl SafeVec {
    pub fn new(size: usize) -> Self {
        Self {
            v: ManuallyDrop::new(vec![0u8; size]),
        }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.v.as_ptr()
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.v.as_mut_ptr()
    }

    /// Returns binary serialized buffer pf a page
    #[inline]
    pub(crate) fn buf(&self) -> &[u8] {
        &self.v
    }

    /// Returns binary serialized muttable buffer of a page
    #[inline]
    pub(crate) fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.v
    }

    /// Returns page size
    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.v.len()
    }

    #[inline]
    pub(crate) unsafe fn get_vec(&mut self, from: usize, end: usize) -> Vec<u8> {
        let cap = self.v.capacity();
        let len = self.v.len();
        let mut ptr = self.v.as_mut_ptr();
        let v = std::vec::Vec::from_raw_parts(ptr, len, cap);
        v
    }

    #[inline]
    pub(crate) unsafe fn get_owned_vec(&mut self, from: usize, sz: usize) -> OwnedVec {
        let v = self.get_vec(from, sz);
        OwnedVec { from, sz, v }
    }
}

pub struct OwnedVec {
    from: usize,
    sz: usize,
    v: Vec<u8>,
}

impl OwnedVec {
    fn get(&self) -> &[u8] {
        &self.v[self.from..self.from + self.sz]
    }

    fn into_vec(self) -> Vec<u8> {
        let v = self.get();
        v.to_vec()
    }
}

#[test]
fn it() {
    let mut buffer = SafeVec::new(1);
    buffer.v[0] = 20;
    for i in 0..1 {
        let mut c1 = unsafe { buffer.get_vec(0, 1) };
        spawn(move || {
            assert_eq!(c1[0], 20);
            // drop(c1);
        })
        .join()
        .unwrap();
    }

    println!("{:?}", buffer.v);

    // let mut join = vec![];
    // for i in 0..100 {
    //     let c = unsafe { buffer.get_vec(0, 1) };
    //
    //     join.push(spawn(move || {
    //         println!("{:?}", c);
    //     }));
    // }
    //
    // for task in join {
    //     task.join().unwrap();
    // }
}
// #![feature(sync_unsafe_cell)]
// use std::cell::SyncUnsafeCell;
// use std::mem::ManuallyDrop;
// use std::thread::spawn;
// use std::sync::Arc;
//
// #[derive(Copy, Clone)]
// struct UnsafeSlice<'a, T> {
//     slice: &'a [SyncUnsafeCell<T>],
// }
//
// unsafe impl<'a, T: Send + Sync> Send for UnsafeSlice<'a, T> {}
// unsafe impl<'a, T: Send + Sync> Sync for UnsafeSlice<'a, T> {}
//
// impl<'a, T> UnsafeSlice<'a, T> {
//     pub fn new(slice: &'a mut [T]) -> Self {
//         let ptr = slice as *mut [T] as *const [SyncUnsafeCell<T>];
//         Self {
//             slice: unsafe { &*ptr },
//         }
//     }
//
//     /// SAFETY: It is UB if two threads write to the same index without
//     /// synchronization.
//     pub unsafe fn write(&self, i: usize, value: T) {
//         let ptr = self.slice[i].get();
//         *ptr = value;
//     }
//
//     /// SAFETY: It is UB if two threads write to the same index without
//     /// synchronization.
//     pub unsafe fn get(&self, i: usize) -> &T {
//         let ptr = self.slice[i].get();
//         (&*ptr).into()
//     }
//
//     pub unsafe fn get_buffer(&self, from: usize, n: usize) -> &[SyncUnsafeCell<T>] {
//         let ptr = &self.slice[from..from + n];
//         ptr
//     }
// }
//
// fn main() {
//     let mut buffer: &'static mut Vec<_>  = &mut vec![0u8; 100];
//     let mut slice = UnsafeSlice::new(&mut buffer);
//
//     let s1 = unsafe { slice.get_buffer(0, 10) };
//     let s2 = unsafe { slice.get_buffer(10, 11) };
//
//     unsafe {
//         slice.write(0, 20);
//         slice.write(21, 39);
//     }
//
//     spawn(move || {
//         println!("{:?}", s1);
//     }).join().unwrap();
//
//     // spawn(move || {
//     //     println!("{:?}", s2);
//     // }).join().unwrap();
// }
