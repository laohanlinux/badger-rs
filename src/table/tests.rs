use std::mem;
use std::pin::pin;
use std::sync::Arc;

#[cfg(test)]
mod utils {
    use crate::options::FileLoadingMode;
    use crate::table::builder::Builder;
    use crate::table::iterator::{BlockIterator, ConcatIterator, IteratorImpl, IteratorItem};
    use crate::table::table::{Table, TableCore, FILE_SUFFIX};
    use crate::y::{open_synced_file, read_at, ValueStruct};
    use crate::{MergeIterOverBuilder, MergeIterOverIterator, Xiterator};
    use memmap::MmapOptions;
    use rand::random;
    use serde_json::ser::CharEscape::Tab;
    use std::borrow::Borrow;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::env::temp_dir;
    use std::fmt::format;
    use std::fs::File;
    use std::io::{Cursor, Seek, SeekFrom, Write};
    use std::process::Output;
    use std::sync::Arc;
    use std::thread::spawn;
    use tokio::io::AsyncSeekExt;
    use tokio_metrics::TaskMetrics;

    #[test]
    fn it_block_iterator() {
        let mut builder = new_builder("anc", 10000);
        let data = builder.finish();
        let it = BlockIterator::new(data);
        let mut i = 0;
        while let Some(item) = it.next() {
            println!(" key: {:?}, value: {:?}", item.key(), item.value());
            i += 1;
        }
        println!("{}", i);
    }

    #[test]
    fn seek_to_fist() {
        let mut joins = vec![];
        for n in vec![101, 199, 200, 250, 9999, 10000] {
            joins.push(spawn(move || {
                let (mut fp, path) = build_test_table("key", n);
                let table = TableCore::open_table(fp, &path, FileLoadingMode::LoadToRADM).unwrap();
            }));
        }

        for join in joins {
            join.join().unwrap();
        }
    }

    #[test]
    fn seek_to_last() {
        for n in vec![101, 199, 200, 250, 9999, 10000] {
            let (mut fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();
            let iter = crate::table::iterator::IteratorImpl::new(Table::new(table), false);
            let value = iter.seek_to_last();
            assert!(value.is_some());
            let v = value.as_ref().unwrap().value();
            assert_eq!(v.value, format!("{}", n - 1).as_bytes());
            assert_eq!(v.meta, 'A' as u8);
            assert_eq!(v.cas_counter, n as u64 - 1);
            let value = iter.prev();
            assert!(value.is_some());
            let v = value.as_ref().unwrap().value();
            assert_eq!(v.value, format!("{}", n - 2).as_bytes());
            assert_eq!(v.meta, 'A' as u8);
            assert_eq!(v.cas_counter, n as u64 - 2);
        }
    }

    #[test]
    fn seek() {
        let (mut fp, path) = build_test_table("k", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();

        let iter = crate::table::iterator::IteratorImpl::new(Table::new(table), false);

        let data = vec![
            ("abc", true, "k0000"),
            ("k0100", true, "k0100"),
            ("k0100b", true, "k0101"),
            ("k1234", true, "k1234"),
            ("k1234b", true, "k1235"),
            ("k9999", true, "k9999"),
            ("z", false, ""),
        ];

        for tt in data {
            let value = iter.seek(tt.0.as_bytes());
            assert_eq!(value.is_some(), tt.1);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().key(), tt.2.as_bytes());
            }
        }
    }

    #[test]
    fn seek_for_prev() {
        let (mut fp, path) = build_test_table("k", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();

        let iter = crate::table::iterator::IteratorImpl::new(Table::new(table), false);
        let data = vec![
            ("abc", false, ""),
            ("k0100", true, "k0100"),
            ("k0100b", true, "k0100"), // Test case where we jump to next block.
            ("k1234", true, "k1234"),
            ("k1234b", true, "k1234"),
            ("k9999", true, "k9999"),
            ("z", true, "k9999"),
        ];

        for tt in data {
            let value = iter.seek_for_prev(tt.0.as_bytes());
            assert_eq!(value.is_some(), tt.1);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().key(), tt.2.as_bytes());
            }
        }
    }

    #[test]
    fn iterator_from_start() {
        for n in vec![101, 199, 200, 250, 9999, 10000] {
            let (mut fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::LoadToRADM).unwrap();
            let iter = crate::table::iterator::IteratorImpl::new(Table::new(table), false);
            iter.reset();
            let mut count = 0;
            let value = iter.seek(b"");
            assert!(value.is_some());
            count += 1;
            // No need to do a Next.
            // ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
            while let Some(value) = iter.next() {
                let value = value.value();
                assert_eq!(value.value, format!("{}", count).as_bytes().to_vec());
                assert_eq!(value.meta, 'A' as u8);
                assert_eq!(value.cas_counter, count as u64);
                count += 1;
            }
            assert_eq!(count, n as isize);
        }
    }

    #[test]
    fn iterator_from_end() {
        for n in vec![101, 199, 200, 250, 9999, 10000] {
            let (mut fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::LoadToRADM).unwrap();
            let iter = crate::table::iterator::IteratorImpl::new(Table::new(table), false);
            iter.reset();
            let value = iter.seek(b"zzzzzz");
            assert!(value.is_none());
            // println!("{} {}", n, iter);

            for i in (0..n).rev() {
                // println!("{} {}", n, iter);
                let value = iter.prev();
                assert!(value.is_some());
                let value = value.as_ref().unwrap().value();
                assert_eq!(value.value, format!("{}", i).as_bytes().to_vec());
                assert_eq!(value.meta, 'A' as u8);
                assert_eq!(value.cas_counter, i as u64);
            }
            assert!(iter.prev().is_none());
        }
    }

    #[test]
    fn table() {
        let (fp, path) = build_test_table("key", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::FileIO).unwrap();
        let iter = crate::table::iterator::IteratorImpl::new(Table::new(table), false);
        let mut kid = 1010;
        let seek = key("key", kid);
        iter.seek(seek.as_bytes());
        kid += 1;
        while let Some(item) = iter.next() {
            assert_eq!(item.key(), key("key", kid).as_bytes());
            kid += 1;
        }

        assert_eq!(kid, 10000, "Expected kid: 10000. Got: {}", kid);
        assert!(iter.seek(key("key", 99999).as_bytes()).is_none());
        assert_eq!(
            iter.seek(key("key", -1).as_bytes()).as_ref().unwrap().key(),
            key("key", 0).as_bytes()
        );
    }

    #[test]
    fn iterate_back_and_forth() {
        let (fp, path) = build_test_table("key", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();

        let seek = key("key", 1010);
        let iter = crate::table::iterator::IteratorImpl::new(Table::new(table), false);
        let item = iter.seek(seek.as_bytes());
        assert!(item.is_some());
        assert_eq!(item.as_ref().unwrap().key(), seek.as_bytes().to_vec());

        iter.prev();
        let item = iter.prev();
        let k = item.as_ref().unwrap().key();
        assert_eq!(k, key("key", 1008).as_bytes());

        iter.next();
        let item = iter.next();
        let k = item.as_ref().unwrap().key();
        assert_eq!(k, seek.as_bytes());

        let item = iter.seek(key("key", 2000).as_bytes());
        let k = item.as_ref().unwrap().key();
        assert_eq!(k, key("key", 2000).as_bytes());

        let item = iter.prev();
        let k = item.as_ref().unwrap().key();
        assert_eq!(k, key("key", 1999).as_bytes());

        let item = iter.seek_to_first();
        let k = item.as_ref().unwrap().key();
        assert_eq!(k, key("key", 0).as_bytes());

        let item = iter.seek_to_last();
        let k = item.as_ref().unwrap().key();
        assert_eq!(k, key("key", 9999).as_bytes());
    }

    #[test]
    fn uni_iterator() {
        let (fp, path) = build_test_table("key", 10000);
        let table =
            Table::new(TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap());

        {
            let iter = crate::table::iterator::IteratorImpl::new(table.clone(), false);
            iter.rewind();
            let mut count = 0;
            while let Some(item) = iter.next() {
                let value = item.value();
                count += 1;
                assert_eq!(format!("{}", count).as_bytes().to_vec(), value.value);
                assert_eq!('A' as u8, value.meta);
                assert_eq!(count, value.cas_counter);
            }
            assert_eq!(count + 1, 10000);
        }

        {
            let iter = crate::table::iterator::IteratorImpl::new(table.clone(), true);
            iter.rewind();
            let mut count = 0;
            while let Some(item) = iter.next() {
                let value = item.value();
                count += 1;
                assert_eq!(
                    format!("{}", 10000 - 1 - count).as_bytes().to_vec(),
                    value.value
                );
                assert_eq!('A' as u8, value.meta);
                assert_eq!(10000 - 1 - count, value.cas_counter);
            }
            assert_eq!(count + 1, 10000);
        }
    }

    #[test]
    fn concat_iterator_one_table() {
        let (fp, path) = build_table(vec![
            (b"k1".to_vec(), b"a1".to_vec()),
            (b"k2".to_vec(), b"a2".to_vec()),
        ]);

        let t1 = Table::new(TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap());
        let table_iter = ConcatIterator::new(vec![t1], false);
        let v = table_iter.rewind().unwrap();
        assert_eq!(v.key(), b"k1");
        assert_eq!(v.value().value, b"a1".to_vec());
        assert_eq!(v.value().meta, 'A' as u8);
    }

    #[test]
    fn concat_iterator() {
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .build_n("keya", 10000);
        let f2 = TableBuilder::new()
            .mode(FileLoadingMode::LoadToRADM)
            .build_n("keyb", 10000);
        let f3 = TableBuilder::new()
            .mode(FileLoadingMode::FileIO)
            .build_n("keyc", 10000);

        {
            let iter = ConcatIterator::new(vec![f1.clone(), f2.clone(), f3.clone()], false);
            assert!(iter.rewind().is_some());
            let mut count = 0;
            while let Some(item) = iter.next() {
                count += 1;
                let value = item.value();
                assert_eq!(format!("{}", count % 10000).as_bytes(), value.value);
            }
            assert_eq!(count + 1, 30000);

            let value = iter.seek(b"a");
            assert_eq!(value.as_ref().unwrap().key(), b"keya0000");
            assert_eq!(value.as_ref().unwrap().value().value, b"0");

            let value = iter.seek(b"keyb");
            assert_eq!(value.as_ref().unwrap().key(), b"keyb0000");
            assert_eq!(value.as_ref().unwrap().value().value, b"0");

            let value = iter.seek(b"keyb9999b");
            assert_eq!(value.as_ref().unwrap().key(), b"keyc0000");
            assert_eq!(value.as_ref().unwrap().value().value, b"0");

            let value = iter.seek(b"keyd");
            assert!(value.is_none());
        }

        {
            let iter = ConcatIterator::new(vec![f1.clone(), f2.clone(), f3.clone()], true);
            assert!(iter.rewind().is_some());
        }
    }

    #[test]
    fn merge_iterator() {
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"a1".to_vec()),
                (b"k2".to_vec(), b"a2".to_vec()),
            ])
            .build();
        let f2 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"b1".to_vec()),
                (b"k2".to_vec(), b"b2".to_vec()),
            ])
            .build();

        let itr1 = Box::new(IteratorImpl::new(f1, false));
        let itr2 = Box::new(ConcatIterator::new(vec![f2], false));
        let mut miter = crate::y::merge_iterator::MergeIterOverBuilder::default()
            .add_batch(vec![itr1, itr2])
            .build();
        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k1");
        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k2");
        let item = miter.next();
        assert!(item.is_none());
        let item = miter.rewind().unwrap();
        assert_eq!(item.key(), b"k1");
        let item = miter.peek().unwrap();
        assert_eq!(item.key(), b"k1");
        assert_eq!(item.value().value, b"a1");
        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k2");
        assert_eq!(item.value().value, b"a2");
        assert!(miter.next().is_none());
    }

    #[test]
    fn merge_iterator_reversed() {
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"a1".to_vec()),
                (b"k2".to_vec(), b"a2".to_vec()),
            ])
            .build();
        let f2 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"b1".to_vec()),
                (b"k2".to_vec(), b"b2".to_vec()),
            ])
            .build();

        let itr1 = Box::new(IteratorImpl::new(f1, true));
        let itr2 = Box::new(ConcatIterator::new(vec![f2], true));
        let mut miter = crate::y::merge_iterator::MergeIterOverBuilder::default()
            .add_batch(vec![itr1, itr2])
            .build();
        let item = miter.rewind().unwrap();
        assert_eq!(item.key(), b"k2");
        assert_eq!(item.value().value, b"a2");
        assert_eq!(item.value().meta, 'A' as u8);
        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k1");
        assert_eq!(item.value().value, b"a1");
        assert_eq!(item.value().meta, 'A' as u8);
        assert!(miter.next().is_none());
    }

    #[test]
    fn merge_iterator_reversed_take_one() {
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"a1".to_vec()),
                (b"k2".to_vec(), b"a2".to_vec()),
            ])
            .build();
        let f2 = TableBuilder::new().mode(FileLoadingMode::MemoryMap).build();

        let itr1 = Box::new(ConcatIterator::new(vec![f1], false));
        let itr2 = Box::new(ConcatIterator::new(vec![f2], false));
        let mut miter = crate::y::merge_iterator::MergeIterOverBuilder::default()
            .add_batch(vec![itr1, itr2])
            .build();

        let item = miter.rewind().unwrap();
        assert_eq!(item.key(), b"k1");
        assert_eq!(item.value().value, b"a1");
        assert_eq!(item.value().meta, 'A' as u8);

        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k2");
        assert_eq!(item.value().value, b"a2");
        assert_eq!(item.value().meta, 'A' as u8);

        assert!(miter.next().is_none());
        assert!(miter.peek().is_none());
    }

    #[test]
    fn merge_iterator_reversed_take_two() {
        let f1 = TableBuilder::new().mode(FileLoadingMode::MemoryMap).build();
        let f2 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"a1".to_vec()),
                (b"k2".to_vec(), b"a2".to_vec()),
            ])
            .build();
        let itr1 = Box::new(ConcatIterator::new(vec![f1], false));
        let itr2 = Box::new(ConcatIterator::new(vec![f2], false));
        let mut miter = crate::y::merge_iterator::MergeIterOverBuilder::default()
            .add_batch(vec![itr1, itr2])
            .build();

        let item = miter.rewind().unwrap();
        assert_eq!(item.key(), b"k1");
        assert_eq!(item.value().value, b"a1");
        assert_eq!(item.value().meta, 'A' as u8);

        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k2");
        assert_eq!(item.value().value, b"a2");
        assert_eq!(item.value().meta, 'A' as u8);

        assert!(miter.next().is_none());
        assert!(miter.peek().is_none());
    }

    #[test]
    fn iter() {
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"a1".to_vec()),
                (b"k2".to_vec(), b"a2".to_vec()),
            ])
            .build();
        let itr = IteratorImpl::new(f1, false);
        let mut mitr = crate::y::merge_iterator::MergeIterOverBuilder::default()
            .add(Box::new(itr))
            .build();
        assert_eq!(mitr.next().unwrap().key(), b"k1");
        assert_eq!(mitr.next().unwrap().key(), b"k2");
        assert!(mitr.next().is_none());
    }

    #[test]
    fn currency() {
        use std::fs;
        let (mut fp, path) = build_test_table("key", 101);
        fp.set_len(0);
        fp.write_all(&vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
        fp.seek(SeekFrom::Start(0)).unwrap();
        let mut joins = vec![];
        for i in 0..10 {
            let mut fp = fp.try_clone().unwrap();
            joins.push(spawn(move || {
                let mut buffer = vec![0u8; 1];
                read_at(&fp, &mut buffer, i).unwrap();
                println!("{:?}", buffer);
            }));
        }

        for join in joins {
            join.join().unwrap();
        }
    }

    #[test]
    fn t_table() {
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"a1".to_vec()),
                (b"k2".to_vec(), b"a2".to_vec()),
            ])
            .build();
        let itr = ConcatIterator::new(vec![f1], false);
    }

    fn build_table(mut key_value: Vec<(Vec<u8>, Vec<u8>)>) -> (File, String) {
        let mut builder = Builder::default();
        let file_name = format!(
            "{}/{}{}",
            temp_dir().to_str().unwrap(),
            random::<u64>(),
            FILE_SUFFIX
        );
        let mut fp = open_synced_file(&file_name, true).unwrap();
        key_value.sort_by(|a, b| a.0.cmp(&b.0));

        for (i, (key, value)) in key_value.iter().enumerate() {
            let got = builder.add(
                key,
                &ValueStruct::new(value.clone(), 'A' as u8, 0, i as u64),
            );
            assert!(got.is_ok());
        }

        fp.write_all(&builder.finish()).unwrap();
        fp.flush().unwrap();
        drop(fp);
        println!("{}", file_name);
        let fp = open_synced_file(&file_name, true).unwrap();
        (fp, file_name)
    }

    fn new_builder(prefix: &str, n: isize) -> Builder {
        assert!(n <= 10000);
        let mut key_values = vec![];
        for i in 0..n {
            let key = key(prefix, i).as_bytes().to_vec();
            let v = format!("{}", i).as_bytes().to_vec();
            key_values.push((key, v));
        }
        let mut builder = Builder::default();
        for (i, (key, value)) in key_values.iter().enumerate() {
            let got = builder.add(
                key,
                &ValueStruct::new(value.clone(), 'A' as u8, 0, i as u64),
            );
            assert!(got.is_ok());
        }

        builder
    }

    fn build_test_table(prefix: &str, n: isize) -> (File, String) {
        assert!(n <= 10000);
        let mut key_values = vec![];
        for i in 0..n {
            let key = key(prefix, i).as_bytes().to_vec();
            let v = format!("{}", i).as_bytes().to_vec();
            key_values.push((key, v));
        }

        build_table(key_values)
    }

    fn key(prefix: &str, n: isize) -> String {
        format!("{}{:04}", prefix, n)
    }

    pub(crate) struct TableBuilder {
        path: String,
        key_value: Vec<(Vec<u8>, Vec<u8>)>,
        mode: FileLoadingMode,
    }

    impl TableBuilder {
        fn new() -> TableBuilder {
            TableBuilder {
                path: "".to_string(),
                // key_prefix: "".to_string(),
                key_value: vec![],
                mode: FileLoadingMode::MemoryMap,
            }
        }

        fn path(mut self, path: String) -> Self {
            self.path = path;
            self
        }

        fn mode(mut self, mode: FileLoadingMode) -> Self {
            self.mode = mode;
            self
        }

        fn key_value(mut self, key_value: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
            self.key_value = key_value;
            self
        }

        // fn prefix(mut self, prefix: &str) -> Self {
        //     self.key_prefix = prefix.to_string();
        //     self
        // }

        fn build(&mut self) -> Table {
            let table = build_table(self.key_value.clone());
            self.path = table.1.clone();
            let t1 = TableCore::open_table(table.0, self.path.as_ref(), FileLoadingMode::MemoryMap)
                .unwrap();
            Table::new(t1)
        }

        fn build_n(&mut self, prefix: &str, n: isize) -> Table {
            let table = build_test_table(prefix, n);
            self.path = table.1.clone();
            let t1 = TableCore::open_table(table.0, self.path.as_ref(), FileLoadingMode::MemoryMap)
                .unwrap();
            Table::new(t1)
        }
    }
}

#[test]
fn await_() {
    use async_trait;
    use tokio; // 1.24.1 // 0.1.61

    pub trait XIterator {}

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            tokio::spawn(async move {
                {
                    let v: Vec<Box<dyn XIterator>> = vec![];
                }

                let v: Vec<Box<dyn XIterator + Send + Sync + 'static>> = vec![];
                let c = tokio::sync::mpsc::channel::<i32>(100);
                c.0.send(100).await;
                100
            });
        });
}
