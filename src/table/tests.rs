#[cfg(test)]
mod utils {
    use crate::options::FileLoadingMode;
    use crate::table::builder::Builder;
    use crate::table::iterator::{
        BlockIterator, ConcatIterator, IteratorImpl, IteratorItem, IteratorSeek,
    };
    use crate::table::table;
    use crate::table::table::{Table, TableCore, FILE_SUFFIX};
    use crate::y::{hex_str, open_synced_file, read_at, ValueStruct};
    use crate::{MergeIterOverBuilder, Xiterator};
    use log::debug;
    use memmap::MmapOptions;
    use rand::random;
    use serde_json::ser::CharEscape::Tab;
    use std::borrow::Borrow;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::env::temp_dir;
    use std::fmt::format;
    use std::fs::File;
    use std::io::{Cursor, Seek, SeekFrom, Write};
    use std::path;
    use std::process::Output;
    use std::sync::Arc;
    use std::thread::spawn;
    use tokio::io::AsyncSeekExt;
    use tokio_metrics::TaskMetrics;

    #[test]
    fn iterator_table() {
        let (fp, path) = build_test_table("key", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::FileIO).unwrap();
        let iter = IteratorImpl::new(Table::new(table), false);
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
    fn iterator_tables() {
        let n = 10000;
        let (fp, path) = build_test_table("key", n);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::FileIO).unwrap();
        let tb = Table::new(table);
        for reverse in [false, true] {
            let itr = IteratorImpl::new(tb.clone(), reverse);
            let mut count = 0;
            let mut keys = HashSet::new();
            while let Some(item) = itr.next() {
                count += 1;
                keys.insert(item.key);
            }
            assert_eq!(count, n);
            assert_eq!(count as usize, keys.len());
        }
        // after rewind
        for reverse in [false, true] {
            let itr = IteratorImpl::new(tb.clone(), reverse);
            let item = itr.rewind().unwrap();
            let mut count = 1;
            let mut keys = HashSet::new();
            keys.insert(item.key);
            while let Some(item) = itr.next() {
                count += 1;
                keys.insert(item.key);
            }
            assert_eq!(count, n);
            assert_eq!(count as usize, keys.len());
        }
    }

    #[test]
    fn block_iterator() {
        {
            let data = new_builder("", 10000).finish();
            let it = BlockIterator::new(data);
            let got = it.seek(format!("{}", 0).as_bytes(), IteratorSeek::Origin);
            assert!(got.is_some());
        }
        {
            let itr = BlockIterator::new(new_builder("anc", 10000).finish());
            let mut i = 0;
            while itr.next().is_some() {
                i += 1;
            }
            // Notice, itr only one block iterator, If you want iterator all keys that should use IteratorImpl
            assert_eq!(i, Builder::RESTART_INTERVAL);
        }
    }

    #[test]
    fn iterator_seek_to_first() {
        crate::test_util::tracing_log();
        for n in [101, 199, 200, 250, 9999, 10000] {
            let (fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::LoadToRADM).unwrap();
            let itr = IteratorImpl::new(Table::new(table), false);
            let value = itr.seek_to_first();
            assert!(value.is_some());
            let v = value.as_ref().unwrap();
            assert_eq!(b"0".as_slice(), &v.value().value);
        }
    }

    #[test]
    fn iterator_seek_to_last() {
        for n in [101, 199, 200, 250, 9999, 10000] {
            let (fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();
            let iter = IteratorImpl::new(Table::new(table), false);
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
    fn iterator_seek() {
        let (fp, path) = build_test_table("k", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();
        let itr = IteratorImpl::new(Table::new(table), false);
        // generate test data
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
            let value = itr.seek(tt.0.as_bytes());
            assert_eq!(value.is_some(), tt.1);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().key(), tt.2.as_bytes());
            }
        }
    }

    #[test]
    fn iterator_seek_for_prev() {
        let (fp, path) = build_test_table("k", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();
        let itr = IteratorImpl::new(Table::new(table), false);
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
            let value = itr.seek_for_prev(tt.0.as_bytes());
            assert_eq!(value.is_some(), tt.1);
            if value.is_some() {
                assert_eq!(value.as_ref().unwrap().key(), tt.2.as_bytes());
            }
        }
    }

    #[test]
    fn iterator_from_start() {
        for n in [101, 199, 200, 250, 9999, 10000] {
            let (fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::LoadToRADM).unwrap();
            let iter = IteratorImpl::new(Table::new(table), false);
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
        for n in [101, 199, 200, 250, 9999, 10000] {
            let (fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::LoadToRADM).unwrap();
            let iter = IteratorImpl::new(Table::new(table), false);
            iter.reset();
            let value = iter.seek(b"zzzzzz");
            assert!(value.is_none());
            for i in (0..n).rev() {
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
    fn iterator_back_and_forth() {
        let (fp, path) = build_test_table("key", 10000);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();

        let seek = key("key", 1010);
        let iter = IteratorImpl::new(Table::new(table), false);
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
    fn concat_iterator_base() {
        let n = 10000;
        let (fp, path) = build_test_table("key", n);
        let table =
            Table::new(TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap());

        {
            let itr = IteratorImpl::new(table.clone(), false);
            itr.rewind();
            let mut count = 1;
            while let Some(item) = itr.next() {
                let value = item.value();
                assert_eq!(format!("{}", count).as_bytes().to_vec(), value.value);
                assert_eq!('A' as u8, value.meta);
                assert_eq!(count, value.cas_counter);
                count += 1;
            }
            assert_eq!(count, n as u64);
        }

        {
            let itr = IteratorImpl::new(table.clone(), false);
            let mut count = 0;
            while let Some(item) = itr.next() {
                let value = item.value();
                assert_eq!(format!("{}", count).as_bytes().to_vec(), value.value);
                count += 1;
            }
            assert_eq!(count, n as u64);
        }

        {
            let itr = IteratorImpl::new(table.clone(), true);
            itr.rewind();
            let mut count = 0;
            while let Some(item) = itr.next() {
                let value = item.value();
                count += 1;
                assert_eq!(
                    format!("{}", n as u64 - 1 - count).as_bytes().to_vec(),
                    value.value
                );
                assert_eq!('A' as u8, value.meta);
                assert_eq!(n as u64 - 1 - count, value.cas_counter);
            }
            assert_eq!(count + 1, n as u64);
        }

        {
            let itr = IteratorImpl::new(table.clone(), true);
            let mut count = 0;
            while let Some(item) = itr.next() {
                let value = item.value();
                assert_eq!(
                    format!("{}", n as u64 - 1 - count).as_bytes().to_vec(),
                    value.value
                );
                count += 1;
            }
            assert_eq!(count, n as u64);
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
    fn concat_iterator_tables() {
        crate::test_util::tracing_log();
        let n = 10000;
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .build_n("keya", n);
        let f2 = TableBuilder::new()
            .mode(FileLoadingMode::LoadToRADM)
            .build_n("keyb", n);
        let f3 = TableBuilder::new()
            .mode(FileLoadingMode::FileIO)
            .build_n("keyc", n);

        {
            let itr = ConcatIterator::new(vec![f1.clone(), f2.clone(), f3.clone()], false);
            let mut count = 0;
            while let Some(item) = itr.next() {
                let value = item.value();
                assert_eq!(format!("{}", count % n).as_bytes(), value.value);
                count += 1;
            }
            assert_eq!(count, 30000);

            let value = itr.seek(b"a");
            assert_eq!(value.as_ref().unwrap().key(), b"keya0000");
            assert_eq!(value.as_ref().unwrap().value().value, b"0");

            let value = itr.seek(b"keyb");
            assert_eq!(value.as_ref().unwrap().key(), b"keyb0000");
            assert_eq!(value.as_ref().unwrap().value().value, b"0");

            let value = itr.seek(b"keyb9999b");
            assert_eq!(value.as_ref().unwrap().key(), b"keyc0000");
            assert_eq!(value.as_ref().unwrap().value().value, b"0");

            let value = itr.seek(b"keyd");
            assert!(value.is_none());
        }

        {
            let itr = ConcatIterator::new(vec![f1.clone(), f2.clone(), f3.clone()], true);
            assert!(itr.rewind().is_some());
        }
    }

    #[test]
    fn merge_iterator_base() {
        crate::test_util::tracing_log();
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
        let miter = MergeIterOverBuilder::default()
            .add_batch(vec![itr1, itr2])
            .build();
        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k1");
        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k2");
        let item = miter.next();
        assert!(item.is_none());
        // Rewind iterator
        let item = miter.rewind().unwrap();
        assert_eq!(item.key(), b"k1");
        let item = miter.peek().unwrap();
        assert_eq!(item.key(), b"k1");
        assert_eq!(item.value().value, b"a1");
        // After next iterator
        let item = miter.next().unwrap();
        assert_eq!(item.key(), b"k2");
        assert_eq!(item.value().value, b"a2");
        assert!(miter.next().is_none());
    }

    #[test]
    fn merge_iterator_reversed_base() {
        crate::test_util::tracing_log();
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
        let miter = MergeIterOverBuilder::default()
            .reverse(true)
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
        let miter = MergeIterOverBuilder::default()
            .reverse(true)
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
    fn merge_iterator_take_two() {
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
        let miter = MergeIterOverBuilder::default()
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
        crate::test_util::tracing_log();
        let f1 = TableBuilder::new()
            .mode(FileLoadingMode::MemoryMap)
            .key_value(vec![
                (b"k1".to_vec(), b"a1".to_vec()),
                (b"k2".to_vec(), b"a2".to_vec()),
            ])
            .build();
        let itr = IteratorImpl::new(f1, false);
        let mitr = MergeIterOverBuilder::default().add(Box::new(itr)).build();
        assert_eq!(mitr.next().unwrap().key(), b"k1");
        assert_eq!(mitr.next().unwrap().key(), b"k2");
        assert!(mitr.next().is_none());

        mitr.rewind();
        let count = mitr.into_iter().count();
        assert_eq!(count, 1);
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
        let mut count = 0;
        while let Some(_) = itr.next() {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn chaos_merge_iterator() {
        use rand::seq::SliceRandom;
        use rand::thread_rng;
        let mut keys = keys();
        keys.shuffle(&mut thread_rng());
        let count = keys
            .iter()
            .map(|key| key.iter().count())
            .fold(0, |acc, count| count + acc);
        let tables = keys
            .into_iter()
            .map(|keys| {
                let keypare = keys
                    .into_iter()
                    .map(|key| (key.clone(), key.clone()))
                    .collect::<Vec<_>>();
                TableBuilder::new().key_value(keypare).build()
            })
            .collect::<Vec<_>>();

        let mut itr: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
        for table in tables {
            let iter = IteratorImpl::new(table, false);
            while let Some(item) = iter.next() {
                println!("=>{}", hex_str(item.key()));
            }
            itr.push(Box::new(iter));
        }
        let mut mitr = MergeIterOverBuilder::default().add_batch(itr).build();
        mitr.rewind();
        let mut _count = 0;
        while let Some(item) = mitr.peek() {
            mitr.next();
            _count += 1;
        }
        assert_eq!(count, _count);
    }

    fn build_table(mut key_value: Vec<(Vec<u8>, Vec<u8>)>) -> (File, String) {
        let mut builder = Builder::default();
        let dir = temp_dir().join(random::<u64>().to_string() + FILE_SUFFIX);
        let file_name = dir.to_str().unwrap();
        let mut fp = open_synced_file(file_name, true).unwrap();
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
        let fp = open_synced_file(file_name, true).unwrap();
        (fp, file_name.to_string())
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
        pre: Option<String>,
    }

    impl TableBuilder {
        fn new() -> TableBuilder {
            TableBuilder {
                path: "".to_string(),
                // key_prefix: "".to_string(),
                key_value: vec![],
                mode: FileLoadingMode::MemoryMap,
                pre: None,
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

        fn prefix(mut self, prefix: String) -> Self {
            self.pre = Some(prefix);
            self
        }

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

    fn keys() -> Vec<Vec<Vec<u8>>> {
        let output = "


            911#912#913#914#915#916#917#918#919#920#921#922#923#924#925#926#927#928#929#930#931#932#933#934#935#936#937#938#939#940#941#942#943#944#945#946#947#948#949#950#951#952#953#954#955#956#957#958#959#960#961#962#963#964#965#966#967#968#969#970#971#972#973#974#975#976#977#978#979#980#981#982#983#984#985#986#987#988#989#990#991#992#993#994#995#996#997#998#999
            608#609#610#611#612#613#614#615#616#617#618#619#620#621#622#623#624#625#626#627#628#629#630#631#632#633#634#635#636#637#638#639#640#641#642#643#644#645#646#647#648#649#650#651#652#653#654#655#656#657#658#659#660#661#662#663#664#665#666#667#668#669#670#671#672#673#674#675#676#677#678#679#680#681#682#683#684#685#686#687#688#689#690#691#692#693#694#695#696#697#698#699#700#701#702#703#704#705#706#707#708#709#710#711#712#713#714#715#716#717#718#719#720#721#722#723#724#725#726#727#728#729#730#731#732#733#734#735#736#737#738#739#740#741#742#743#744#745#746#747#748#749#750#751#752#753#754#755#756#757#758#759#760#761#762#763#764#765#766#767#768#769#770#771#772#773#774#775#776#777#778#779#780#781#782#783#784#785#786#787#788#789#790#791#792#793#794#795#796#797#798#799#800#801#802#803#804#805#806#807#808#809#810#811#812#813#814#815#816#817#818#819#820#821#822#823#824#825#826#827#828#829#830#831#832#833#834#835#836#837#838#839#840#841#842#843#844#845#846#847#848#849#850#851#852#853#854#855#856#857#858#859#860#861#862#863#864#865#866#867#868#869#870#871#872#873#874#875#876#877#878#879#880#881#882#883#884#885#886#887#888#889#890#891#892#893#894#895#896#897#898#899#900#901#902#903#904#905#906#907#908#909
            323#324#325#326#327#328#329#330#331#332#333#334#335#336#337#338#339#340#341#342#343#344#345#346#347#348#349#350#351#352#353#354#355#356#357#358#359#360#361#362#363#364#365#366#367#368#369#370#371#372#373#374#375#376#377#378#379#380#381#382#383#384#385#386#387#388#389#390#391#392#393#394#395#396#397#398#399#400#401#402#403#404#405#406#407#408#409#410#411#412#413#414#415#416#417#418#419#420#421#422#423#424#425#426#427#428#429#430#431#432#433#434#435#436#437#438#439#440#441#442#443#444#445#446#447#448#449#450#451#452#453#454#455#456#457#458#459#460#461#462#463#464#465#466#467#468#469#470#471#472#473#474#475#476#477#478#479#480#481#482#483#484#485#486#487#488#489#490#491#492#493#494#495#496#497#498#499#500#501#502#503#504#505#506#507#508#509#510#511#512#513#514#515#516#517#518#519#520#521#522#523#524#525#526#527#528#529#530#531#532#533#534#535#536#537#538#539#540#541#542#543#544#545#546#547#548#549#550#551#552#553#554#555#556#557#558#559#560#561#562#563#564#565#566#567#568#569#570#571#572#573#574#575#576#577#578#579#580#581#582#583#584#585#586#587#588#589#590#591#592#593#594#595#596#597#598#599#600#601#602#603#604#605#606
            34#35#36#37#38#39#4#40#41#42#43#44#45#46#47#48#49#5#50#51#52#53#54#55#56#57#58#59#6#60#61#62#63#64#65#66#67#68#69#7#70#71#72#73#74#75#76#77#78#79#8#80#81#82#83#84#85#86#87#88#89#9#90#91#92#93#94#95#96#97#98#99
            9698#9699#9700#9701#9702#9703#9704#9705#9706#9707#9708#9709#9710#9711#9712#9713#9714#9715#9716#9717#9718#9719#9720#9721#9722#9723#9724#9725#9726#9727#9728#9729#9730#9731#9732#9733#9734#9735#9736#9737#9738#9739#9740#9741#9742#9743#9744#9745#9746#9747#9748#9749#9750#9751#9752#9753#9754#9755#9756#9757#9758#9759#9760#9761#9762#9763#9764#9765#9766#9767#9768#9769#9770#9771#9772#9773#9774#9775#9776#9777#9778#9779#9780#9781#9782#9783#9784#9785#9786#9787#9788#9789#9790#9791#9792#9793#9794#9795#9796#9797#9798#9799#9800#9801#9802#9803#9804#9805#9806#9807#9808#9809#9810#9811#9812#9813#9814#9815#9816#9817#9818#9819#9820#9821#9822#9823#9824#9825#9826#9827#9828#9829#9830#9831#9832#9833#9834#9835#9836#9837#9838#9839#9840#9841#9842#9843#9844#9845#9846#9847#9848#9849#9850#9851#9852#9853#9854#9855#9856#9857#9858#9859#9860#9861#9862#9863#9864#9865#9866#9867#9868#9869#9870#9871#9872#9873#9874#9875#9876#9877#9878#9879#9880#9881#9882#9883#9884#9885#9886#9887#9888#9889#9890#9891#9892#9893#9894#9895#9896#9897#9898#9899#9900#9901#9902#9903#9904#9905#9906#9907#9908#9909#9910#9911#9912#9913#9914#9915#9916#9917#9918#9919#9920#9921#9922#9923#9924#9925#9926#9927#9928#9929#9930#9931#9932#9933#9934#9935#9936#9937#9938#9939#9940#9941#9942#9943#9944#9945#9946#9947#9948#9949#9950#9951#9952#9953#9954#9955#9956#9957#9958#9959#9960#9961#9962#9963#9964#9965#9966#9967#9968#9969#9970#9971#9972#9973#9974#9975#9976#9977#9978#9979#9980#9981#9982#9983#9984#9985#9986#9987#9988#9989#9990#9991#9992#9993#9994#9995#9996#9997#9998#9999
            9395#9396#9397#9398#9399#9400#9401#9402#9403#9404#9405#9406#9407#9408#9409#9410#9411#9412#9413#9414#9415#9416#9417#9418#9419#9420#9421#9422#9423#9424#9425#9426#9427#9428#9429#9430#9431#9432#9433#9434#9435#9436#9437#9438#9439#9440#9441#9442#9443#9444#9445#9446#9447#9448#9449#9450#9451#9452#9453#9454#9455#9456#9457#9458#9459#9460#9461#9462#9463#9464#9465#9466#9467#9468#9469#9470#9471#9472#9473#9474#9475#9476#9477#9478#9479#9480#9481#9482#9483#9484#9485#9486#9487#9488#9489#9490#9491#9492#9493#9494#9495#9496#9497#9498#9499#9500#9501#9502#9503#9504#9505#9506#9507#9508#9509#9510#9511#9512#9513#9514#9515#9516#9517#9518#9519#9520#9521#9522#9523#9524#9525#9526#9527#9528#9529#9530#9531#9532#9533#9534#9535#9536#9537#9538#9539#9540#9541#9542#9543#9544#9545#9546#9547#9548#9549#9550#9551#9552#9553#9554#9555#9556#9557#9558#9559#9560#9561#9562#9563#9564#9565#9566#9567#9568#9569#9570#9571#9572#9573#9574#9575#9576#9577#9578#9579#9580#9581#9582#9583#9584#9585#9586#9587#9588#9589#9590#9591#9592#9593#9594#9595#9596#9597#9598#9599#9600#9601#9602#9603#9604#9605#9606#9607#9608#9609#9610#9611#9612#9613#9614#9615#9616#9617#9618#9619#9620#9621#9622#9623#9624#9625#9626#9627#9628#9629#9630#9631#9632#9633#9634#9635#9636#9637#9638#9639#9640#9641#9642#9643#9644#9645#9646#9647#9648#9649#9650#9651#9652#9653#9654#9655#9656#9657#9658#9659#9660#9661#9662#9663#9664#9665#9666#9667#9668#9669#9670#9671#9672#9673#9674#9675#9676#9677#9678#9679#9680#9681#9682#9683#9684#9685#9686#9687#9688#9689#9690#9691#9692#9693#9694#9695#9696
            9092#9093#9094#9095#9096#9097#9098#9099#9100#9101#9102#9103#9104#9105#9106#9107#9108#9109#9110#9111#9112#9113#9114#9115#9116#9117#9118#9119#9120#9121#9122#9123#9124#9125#9126#9127#9128#9129#9130#9131#9132#9133#9134#9135#9136#9137#9138#9139#9140#9141#9142#9143#9144#9145#9146#9147#9148#9149#9150#9151#9152#9153#9154#9155#9156#9157#9158#9159#9160#9161#9162#9163#9164#9165#9166#9167#9168#9169#9170#9171#9172#9173#9174#9175#9176#9177#9178#9179#9180#9181#9182#9183#9184#9185#9186#9187#9188#9189#9190#9191#9192#9193#9194#9195#9196#9197#9198#9199#9200#9201#9202#9203#9204#9205#9206#9207#9208#9209#9210#9211#9212#9213#9214#9215#9216#9217#9218#9219#9220#9221#9222#9223#9224#9225#9226#9227#9228#9229#9230#9231#9232#9233#9234#9235#9236#9237#9238#9239#9240#9241#9242#9243#9244#9245#9246#9247#9248#9249#9250#9251#9252#9253#9254#9255#9256#9257#9258#9259#9260#9261#9262#9263#9264#9265#9266#9267#9268#9269#9270#9271#9272#9273#9274#9275#9276#9277#9278#9279#9280#9281#9282#9283#9284#9285#9286#9287#9288#9289#9290#9291#9292#9293#9294#9295#9296#9297#9298#9299#9300#9301#9302#9303#9304#9305#9306#9307#9308#9309#9310#9311#9312#9313#9314#9315#9316#9317#9318#9319#9320#9321#9322#9323#9324#9325#9326#9327#9328#9329#9330#9331#9332#9333#9334#9335#9336#9337#9338#9339#9340#9341#9342#9343#9344#9345#9346#9347#9348#9349#9350#9351#9352#9353#9354#9355#9356#9357#9358#9359#9360#9361#9362#9363#9364#9365#9366#9367#9368#9369#9370#9371#9372#9373#9374#9375#9376#9377#9378#9379#9380#9381#9382#9383#9384#9385#9386#9387#9388#9389#9390#9391#9392#9393
            3221#3222#3223#3224#3225#3226#3227#3228#3229#323#3230#3231#3232#3233#3234#3235#3236#3237#3238#3239#324#3240#3241#3242#3243#3244#3245#3246#3247#3248#3249#325#3250#3251#3252#3253#3254#3255#3256#3257#3258#3259#326#3260#3261#3262#3263#3264#3265#3266#3267#3268#3269#327#3270#3271#3272#3273#3274#3275#3276#3277#3278#3279#328#3280#3281#3282#3283#3284#3285#3286#3287#3288#3289#329#3290#3291#3292#3293#3294#3295#3296#3297#3298#3299#33#330#3300#3301#3302#3303#3304#3305#3306#3307#3308#3309#331#3310#3311#3312#3313#3314#3315#3316#3317#3318#3319#332#3320#3321#3322#3323#3324#3325#3326#3327#3328#3329#333#3330#3331#3332#3333#3334#3335#3336#3337#3338#3339#334#3340#3341#3342#3343#3344#3345#3346#3347#3348#3349#335#3350#3351#3352#3353#3354#3355#3356#3357#3358#3359#336#3360#3361#3362#3363#3364#3365#3366#3367#3368#3369#337#3370#3371#3372#3373#3374#3375#3376#3377#3378#3379#338#3380#3381#3382#3383#3384#3385#3386#3387#3388#3389#339#3390#3391#3392#3393#3394#3395#3396#3397#3398#3399#34#340#3400#3401#3402#3403#3404#3405#3406#3407#3408#3409#341#3410#3411#3412#3413#3414#3415#3416#3417#3418#3419#342#3420#3421#3422#3423#3424#3425#3426#3427#3428#3429#343#3430#3431#3432#3433#3434#3435#3436#3437#3438#3439#344#3440#3441#3442#3443#3444#3445#3446#3447#3448#3449#345#3450#3451#3452#3453#3454#3455#3456#3457#3458#3459#346#3460#3461#3462#3463#3464#3465#3466#3467#3468#3469#347#3470#3471#3472#3473#3474#3475#3476#3477#3478#3479#348#3480#3481#3482#3483#3484#3485#3486#3487#3488#3489#349#3490#3491#3492#3493#3494#3495#3496#3497#3498#3499#35#350#3500#3501#3502#3503#3504#3505#3506#3507#3508#3509#351#3510#3511#3512#3513#3514#3515#3516#3517#3518#3519#352#3520#3521#3522#3523#3524#3525#3526#3527#3528#3529#353#3530#3531#3532#3533#3534#3535#3536#3537#3538#3539#354#3540#3541#3542#3543#3544#3545#3546#3547#3548#3549#355#3550#3551#3552#3553#3554#3555#3556#3557#3558#3559#356#3560#3561#3562#3563#3564#3565#3566#3567#3568#3569#357#3570#3571#3572#3573#3574#3575#3576#3577#3578#3579#358#3580#3581#3582#3583#3584#3585#3586#3587#3588#3589#359#3590#3591#3592#3593#3594#3595#3596#3597#3598#3599#36#360#3600#3601#3602#3603#3604#3605#3606#3607#3608#3609#361#3610#3611#3612#3613#3614#3615#3616#3617#3618#3619#362#3620#3621#3622#3623#3624#3625#3626#3627#3628#3629#363#3630#3631#3632#3633#3634#3635#3636#3637#3638#3639#364#3640#3641#3642#3643#3644#3645#3646#3647#3648#3649#365#3650#3651#3652#3653#3654#3655#3656#3657#3658#3659#366#3660#3661#3662#3663#3664#3665#3666#3667#3668#3669#367#3670#3671#3672#3673#3674#3675#3676#3677#3678#3679#368#3680#3681#3682#3683#3684#3685#3686#3687#3688#3689#369#3690#3691#3692#3693#3694#3695#3696#3697#3698#3699#37#370#3700#3701#3702#3703#3704#3705#3706#3707#3708#3709#371#3710#3711#3712#3713#3714#3715#3716#3717#3718#3719#372#3720#3721#3722#3723#3724#3725#3726#3727#3728#3729#373#3730#3731#3732#3733#3734#3735#3736#3737#3738#3739#374#3740#3741#3742#3743#3744#3745#3746#3747#3748#3749#375#3750#3751#3752#3753#3754#3755#3756#3757#3758#3759#376#3760#3761#3762#3763#3764#3765#3766#3767#3768#3769#377#3770#3771#3772#3773#3774#3775#3776#3777#3778#3779#378#3780#3781#3782#3783#3784#3785#3786#3787#3788#3789#379#3790#3791#3792#3793#3794#3795#3796#3797#3798#3799#38#380#3800#3801#3802#3803#3804#3805#3806#3807#3808#3809#381#3810#3811#3812#3813#3814#3815#3816#3817#3818#3819#382#3820#3821#3822#3823#3824#3825#3826#3827#3828#3829#383#3830#3831#3832#3833#3834#3835#3836#3837#3838#3839#384#3840#3841#3842#3843#3844#3845#3846#3847#3848#3849#385#3850#3851#3852#3853#3854#3855#3856#3857#3858#3859#386#3860#3861#3862#3863#3864#3865#3866#3867#3868#3869#387#3870#3871#3872#3873#3874#3875#3876#3877#3878#3879#388#3880#3881#3882#3883#3884#3885#3886#3887#3888#3889#389#3890#3891#3892#3893#3894#3895#3896#3897#3898#3899#39#390#3900#3901#3902#3903#3904#3905#3906#3907#3908#3909#391#3910#3911#3912#3913#3914#3915#3916#3917#3918#3919#392#3920#3921#3922#3923#3924#3925#3926#3927#3928#3929#393#3930#3931#3932#3933#3934#3935#3936#3937#3938#3939#394#3940#3941#3942#3943#3944#3945#3946#3947#3948#3949#395#3950#3951#3952#3953#3954#3955#3956#3957#3958#3959#396#3960#3961#3962#3963#3964#3965#3966#3967#3968#3969#397#3970#3971#3972#3973#3974#3975#3976#3977#3978#3979#398#3980#3981#3982#3983#3984#3985#3986#3987#3988#3989#399#3990#3991#3992#3993#3994#3995#3996#3997#3998#3999#4#40#400#4000#4001#4002#4003#4004#4005#4006#4007#4008#4009#401#4010#4011#4012#4013#4014#4015#4016#4017#4018#4019#402#4020#4021#4022#4023#4024#4025#4026#4027#4028#4029#403#4030#4031#4032#4033#4034#4035#4036#4037#4038#4039#404#4040#4041#4042#4043#4044#4045#4046#4047#4048#4049#405#4050#4051#4052#4053#4054#4055#4056#4057#4058#4059#406#4060#4061#4062#4063#4064#4065#4066#4067#4068#4069#407#4070#4071#4072#4073#4074#4075#4076#4077#4078#4079#408#4080#4081#4082#4083#4084#4085#4086#4087#4088#4089#409#4090#4091#4092#4093#4094#4095#4096#4097#4098#4099#41#410#4100#4101#4102#4103#4104#4105#4106#4107#4108#4109#411#4110#4111#4112#4113#4114#4115#4116#4117#4118#4119#412#4120#4121#4122#4123#4124#4125#4126#4127#4128#4129#413#4130#4131#4132#4133#4134#4135#4136#4137#4138#4139#414#4140#4141#4142#4143#4144#4145#4146#4147#4148#4149#415#4150#4151#4152#4153#4154#4155#4156#4157#4158#4159#416#4160#4161#4162#4163#4164#4165#4166#4167#4168#4169#417#4170#4171#4172#4173#4174#4175#4176#4177#4178#4179#418#4180#4181#4182#4183#4184#4185#4186#4187#4188#4189#419#4190#4191#4192#4193#4194#4195#4196#4197#4198#4199#42#420#4200#4201#4202#4203#4204#4205#4206#4207#4208#4209#421#4210#4211#4212#4213#4214#4215#4216#4217#4218#4219#422#4220#4221#4222#4223#4224#4225#4226#4227#4228#4229#423#4230#4231#4232#4233#4234#4235#4236#4237#4238#4239#424#4240#4241#4242#4243#4244#4245#4246#4247#4248#4249#425#4250#4251#4252#4253#4254#4255#4256#4257#4258#4259#426#4260#4261#4262#4263#4264#4265#4266#4267#4268#4269#427#4270#4271#4272#4273#4274#4275#4276#4277#4278#4279#428#4280#4281#4282#4283#4284#4285#4286#4287#4288#4289#429#4290#4291#4292#4293#4294#4295#4296#4297#4298#4299#43#430#4300#4301#4302#4303#4304#4305#4306#4307#4308#4309#431#4310#4311#4312#4313#4314#4315#4316#4317#4318#4319#432#4320#4321#4322#4323#4324#4325#4326#4327#4328#4329#433#4330#4331#4332#4333#4334#4335#4336#4337#4338#4339#434#4340#4341#4342#4343#4344#4345#4346#4347#4348#4349#435#4350#4351#4352#4353#4354#4355#4356#4357#4358#4359#436#4360#4361#4362#4363#4364#4365#4366#4367#4368#4369#437#4370#4371#4372#4373#4374#4375#4376#4377#4378#4379#438#4380#4381#4382#4383#4384#4385#4386#4387#4388#4389#439#4390#4391#4392#4393#4394#4395#4396#4397#4398#4399#44#440#4400#4401#4402#4403#4404#4405#4406#4407#4408#4409#441#4410#4411#4412#4413#4414#4415#4416#4417#4418#4419#442#4420#4421#4422#4423#4424#4425#4426#4427#4428#4429#443#4430#4431#4432#4433#4434#4435#4436#4437#4438#4439#444#4440#4441#4442#4443#4444#4445#4446#4447#4448#4449#445#4450#4451#4452#4453#4454#4455#4456#4457#4458#4459#446#4460#4461#4462#4463#4464#4465#4466#4467#4468#4469#447#4470#4471#4472#4473#4474#4475#4476#4477#4478#4479#448#4480#4481#4482#4483#4484#4485#4486#4487#4488#4489#449#4490#4491#4492#4493#4494#4495#4496#4497#4498#4499#45#450#4500#4501#4502#4503#4504#4505#4506#4507#4508#4509#451#4510#4511#4512#4513#4514#4515#4516#4517#4518#4519#452#4520#4521#4522#4523#4524#4525#4526#4527#4528#4529#453#4530#4531#4532#4533#4534#4535#4536#4537#4538#4539#454#4540#4541#4542#4543#4544#4545#4546#4547#4548#4549#455#4550#4551#4552#4553#4554#4555#4556#4557#4558#4559#456#4560#4561#4562#4563#4564#4565#4566#4567#4568#4569#457#4570#4571#4572#4573#4574#4575#4576#4577#4578#4579#458#4580#4581#4582#4583#4584#4585#4586#4587#4588#4589#459#4590#4591#4592#4593#4594#4595#4596#4597#4598#4599#46#460#4600#4601#4602#4603#4604#4605#4606#4607#4608#4609#461#4610#4611#4612#4613#4614#4615#4616#4617#4618#4619#462#4620#4621#4622#4623#4624#4625#4626#4627#4628#4629#463#4630#4631#4632#4633#4634#4635#4636#4637#4638#4639#464#4640#4641#4642#4643#4644#4645#4646#4647#4648#4649#465#4650#4651#4652#4653#4654#4655#4656#4657#4658#4659#466#4660#4661#4662#4663#4664#4665#4666#4667#4668#4669#467#4670#4671#4672#4673#4674#4675#4676#4677#4678#4679#468#4680#4681#4682#4683#4684#4685#4686#4687#4688#4689#469#4690#4691#4692#4693#4694#4695#4696#4697#4698#4699#47#470#4700#4701#4702#4703#4704#4705#4706#4707#4708#4709#471#4710#4711#4712#4713#4714#4715#4716#4717#4718#4719#472#4720#4721#4722#4723#4724#4725#4726#4727#4728#4729#473#4730#4731#4732#4733#4734#4735#4736#4737#4738#4739#474#4740#4741#4742#4743#4744#4745#4746#4747#4748#4749#475#4750#4751#4752#4753#4754#4755#4756#4757#4758#4759#476#4760#4761#4762#4763#4764#4765#4766#4767#4768#4769#477#4770#4771#4772#4773#4774#4775#4776#4777#4778#4779#478#4780#4781#4782#4783#4784#4785#4786#4787#4788#4789#479#4790#4791#4792#4793#4794#4795#4796#4797#4798#4799#48#480#4800#4801#4802#4803#4804#4805#4806#4807#4808#4809#481#4810#4811#4812#4813#4814#4815#4816#4817#4818#4819#482#4820#4821#4822#4823#4824#4825#4826#4827#4828#4829#483#4830#4831#4832#4833#4834#4835#4836#4837#4838#4839#484#4840#4841#4842#4843#4844#4845#4846#4847#4848#4849#485#4850#4851#4852#4853#4854#4855#4856#4857#4858#4859#486#4860#4861#4862#4863#4864#4865#4866#4867#4868#4869#487#4870#4871#4872#4873#4874#4875#4876#4877#4878#4879#488#4880#4881#4882#4883#4884#4885#4886#4887#4888#4889#489#4890#4891#4892#4893#4894#4895#4896#4897#4898#4899#49#490#4900#4901#4902#4903#4904#4905#4906#4907#4908#4909#491#4910#4911#4912#4913#4914#4915#4916#4917#4918#4919#492#4920#4921#4922#4923#4924#4925#4926#4927#4928#4929#493#4930#4931#4932#4933#4934#4935#4936#4937#4938#4939#494#4940#4941#4942#4943#4944#4945#4946#4947#4948#4949#495#4950#4951#4952#4953#4954#4955#4956#4957#4958#4959#496#4960#4961#4962#4963#4964#4965#4966#4967#4968#4969#497#4970#4971#4972#4973#4974#4975#4976#4977#4978#4979#498#4980#4981#4982#4983#4984#4985#4986#4987#4988#4989#499#4990#4991#4992#4993#4994#4995#4996#4997#4998#4999#5#50#500#5000#5001#5002#5003#5004#5005#5006#5007#5008#5009#501#5010#5011#5012#5013#5014#5015#5016#5017#5018#5019#502#5020#5021#5022#5023#5024#5025#5026#5027#5028#5029#503#5030#5031#5032#5033#5034#5035#5036#5037#5038#5039#504#5040#5041#5042#5043#5044#5045#5046#5047#5048#5049#505#5050#5051#5052#5053#5054#5055#5056#5057#5058#5059#506#5060#5061#5062#5063#5064#5065#5066#5067#5068#5069#507#5070#5071#5072#5073#5074#5075#5076#5077#5078#5079#508#5080#5081#5082#5083#5084#5085#5086#5087#5088#5089#509#5090#5091#5092#5093#5094#5095#5096#5097#5098#5099#51#510#5100#5101#5102#5103#5104#5105#5106#5107#5108#5109#511#5110#5111#5112#5113#5114#5115#5116#5117#5118#5119#512#5120#5121#5122#5123#5124#5125#5126#5127#5128#5129#513#5130#5131#5132#5133#5134#5135#5136#5137#5138#5139#514#5140#5141#5142#5143#5144#5145#5146#5147#5148#5149#515#5150#5151#5152#5153#5154#5155#5156#5157#5158#5159#516#5160#5161#5162#5163#5164#5165#5166#5167#5168#5169#517#5170#5171#5172#5173#5174#5175#5176#5177#5178#5179#518#5180#5181#5182#5183#5184#5185#5186#5187#5188#5189#519#5190#5191#5192#5193#5194#5195#5196#5197#5198#5199#52#520#5200#5201#5202#5203#5204#5205#5206#5207#5208#5209#521#5210#5211#5212#5213#5214#5215#5216#5217#5218#5219#522#5220#5221#5222#5223#5224#5225#5226#5227#5228#5229#523#5230#5231#5232#5233#5234#5235#5236#5237#5238#5239#524#5240#5241#5242#5243#5244#5245#5246#5247#5248#5249#525#5250#5251#5252#5253#5254#5255#5256#5257#5258#5259#526#5260#5261#5262#5263#5264#5265#5266#5267#5268#5269#527#5270#5271#5272#5273#5274#5275#5276#5277#5278#5279#528#5280#5281#5282#5283#5284#5285#5286#5287#5288#5289#529#5290#5291#5292#5293#5294#5295#5296#5297#5298#5299#53#530#5300#5301#5302#5303#5304#5305#5306#5307#5308#5309#531#5310#5311#5312#5313#5314#5315#5316#5317#5318#5319#532#5320#5321#5322#5323#5324#5325#5326#5327#5328#5329#533#5330#5331#5332#5333#5334#5335#5336#5337#5338#5339#534#5340#5341#5342#5343#5344#5345#5346#5347#5348#5349#535#5350#5351#5352#5353#5354#5355#5356#5357#5358#5359#536#5360#5361#5362#5363#5364#5365#5366#5367#5368#5369#537#5370#5371#5372#5373#5374#5375#5376#5377#5378#5379#538#5380#5381#5382#5383#5384#5385#5386#5387#5388#5389#539#5390#5391#5392#5393#5394#5395#5396#5397#5398#5399#54#540#5400#5401#5402#5403#5404#5405#5406#5407#5408#5409#541#5410#5411#5412#5413#5414#5415#5416#5417#5418#5419#542#5420#5421#5422#5423#5424#5425#5426#5427#5428#5429#543#5430#5431#5432#5433#5434#5435#6545#6546#6547#6548#6549#655#6550#6551#6552#6553#6554#6555#6556#6557#6558#6559#656#6560#6561#6562#6563#6564#6565#6566#6567#6568#6569#657#6570#6571#6572#6573#6574#6575#6576#6577#6578#6579#658#6580#6581#6582#6583#6584#6585#6586#6587#6588#6589#659#6590#6591#6592#6593#6594#6595#6596#6597#6598#6599#66#660#6600#6601#6602#6603#6604#6605#6606#6607#6608#6609#661#6610#6611#6612#6613#6614#6615#6616#6617#6618#6619#662#6620#6621#6622#6623#6624#6625#6626#6627#6628#6629#663#6630#6631#6632#6633#6634#6635#6636#6637#6638#6639#664#6640#6641#6642#6643#6644#6645#6646#6647#6648#6649#665#6650#6651#6652#6653#6654#6655#6656#6657#6658#6659#666#6660#6661#6662#6663#6664#6665#6666#6667#6668#6669#667#6670#6671#6672#6673#6674#6675#6676#6677#6678#6679#668#6680#6681#6682#6683#6684#6685#6686#6687#6688#6689#669#6690#6691#6692#6693#6694#6695#6696#6697#6698#6699#67#670#6700#6701#6702#6703#6704#6705#6706#6707#6708#6709#671#6710#6711#6712#6713#6714#6715#6716#6717#6718#6719#672#6720#6721#6722#6723#6724#6725#6726#6727#6728#6729#673#6730#6731#6732#6733#6734#6735#6736#6737#6738#6739#674#6740#6741#6742#6743#6744#6745#6746#6747#6748#6749#675#6750#6751#6752#6753#6754#6755#6756#6757#6758#6759#676#6760#6761#6762#6763#6764#6765#6766#6767#6768#6769#677#6770#6771#6772#6773#6774#6775#6776#6777#6778#6779#678#6780#6781#6782#6783#6784#6785#6786#6787#6788#6789#679#6790#6791#6792#6793#6794#6795#6796#6797#6798#6799#68#680#6800#6801#6802#6803#6804#6805#6806#6807#6808#6809#681#6810#6811#6812#6813#6814#6815#6816#6817#6818#6819#682#6820#6821#6822#6823#6824#6825#6826#6827#6828#6829#683#6830#6831#6832#6833#6834#6835#6836#6837#6838#6839#684#6840#6841#6842#6843#6844#6845#6846#6847#6848#6849#685#6850#6851#6852#6853#6854#6855#6856#6857#6858#6859#686#6860#6861#6862#6863#6864#6865#6866#6867#6868#6869#687#6870#6871#6872#6873#6874#6875#6876#6877#6878#6879#688#6880#6881#6882#6883#6884#6885#6886#6887#6888#6889#689#6890#6891#6892#6893#6894#6895#6896#6897#6898#6899#69#690#6900#6901#6902#6903#6904#6905#6906#6907#6908#6909#691#6910#6911#6912#6913#6914#6915#6916#6917#6918#6919#692#6920#6921#6922#6923#6924#6925#6926#6927#6928#6929#693#6930#6931#6932#6933#6934#6935#6936#6937#6938#6939#694#6940#6941#6942#6943#6944#6945#6946#6947#6948#6949#695#6950#6951#6952#6953#6954#6955#6956#6957#6958#6959#696#6960#6961#6962#6963#6964#6965#6966#6967#6968#6969#697#6970#6971#6972#6973#6974#6975#6976#6977#6978#6979#698#6980#6981#6982#6983#6984#6985#6986#6987#6988#6989#699#6990#6991#6992#6993#6994#6995#6996#6997#6998#6999#7#70#700#7000#7001#7002#7003#7004#7005#7006#7007#7008#7009#701#7010#7011#7012#7013#7014#7015#7016#7017#7018#7019#702#7020#7021#7022#7023#7024#7025#7026#7027#7028#7029#703#7030#7031#7032#7033#7034#7035#7036#7037#7038#7039#704#7040#7041#7042#7043#7044#7045#7046#7047#7048#7049#705#7050#7051#7052#7053#7054#7055#7056#7057#7058#7059#706#7060#7061#7062#7063#7064#7065#7066#7067#7068#7069#707#7070#7071#7072#7073#7074#7075#7076#7077#7078#7079#708#7080#7081#7082#7083#7084#7085#7086#7087#7088#7089#709#7090#7091#7092#7093#7094#7095#7096#7097#7098#7099#71#710#7100#7101#7102#7103#7104#7105#7106#7107#7108#7109#711#7110#7111#7112#7113#7114#7115#7116#7117#7118#7119#712#7120#7121#7122#7123#7124#7125#7126#7127#7128#7129#713#7130#7131#7132#7133#7134#7135#7136#7137#7138#7139#714#7140#7141#7142#7143#7144#7145#7146#7147#7148#7149#715#7150#7151#7152#7153#7154#7155#7156#7157#7158#7159#716#7160#7161#7162#7163#7164#7165#7166#7167#7168#7169#717#7170#7171#7172#7173#7174#7175#7176#7177#7178#7179#718#7180#7181#7182#7183#7184#7185#7186#7187#7188#7189#719#7190#7191#7192#7193#7194#7195#7196#7197#7198#7199#72#720#7200#7201#7202#7203#7204#7205#7206#7207#7208#7209#721#7210#7211#7212#7213#7214#7215#7216#7217#7218#7219#722#7220#7221#7222#7223#7224#7225#7226#7227#7228#7229#723#7230#7231#7232#7233#7234#7235#7236#7237#7238#7239#724#7240#7241#7242#7243#7244#7245#7246#7247#7248#7249#725#7250#7251#7252#7253#7254#7255#7256#7257#7258#7259#726#7260#7261#7262#7263#7264#7265#7266#7267#7268#7269#727#7270#7271#7272#7273#7274#7275#7276#7277#7278#7279#728#7280#7281#7282#7283#7284#7285#7286#7287#7288#7289#729#7290#7291#7292#7293#7294#7295#7296#7297#7298#7299#73#730#7300#7301#7302#7303#7304#7305#7306#7307#7308#7309#731#7310#7311#7312#7313#7314#7315#7316#7317#7318#7319#732#7320#7321#7322#7323#7324#7325#7326#7327#7328#7329#733#7330#7331#7332#7333#7334#7335#7336#7337#7338#7339#734#7340#7341#7342#7343#7344#7345#7346#7347#7348#7349#735#7350#7351#7352#7353#7354#7355#7356#7357#7358#7359#736#7360#7361#7362#7363#7364#7365#7366#7367#7368#7369#737#7370#7371#7372#7373#7374#7375#7376#7377#7378#7379#738#7380#7381#7382#7383#7384#7385#7386#7387#7388#7389#739#7390#7391#7392#7393#7394#7395#7396#7397#7398#7399#74#740#7400#7401#7402#7403#7404#7405#7406#7407#7408#7409#741#7410#7411#7412#7413#7414#7415#7416#7417#7418#7419#742#7420#7421#7422#7423#7424#7425#7426#7427#7428#7429#743#7430#7431#7432#7433#7434#7435#7436#7437#7438#7439#744#7440#7441#7442#7443#7444#7445#7446#7447#7448#7449#745#7450#7451#7452#7453#7454#7455#7456#7457#7458#7459#746#7460#7461#7462#7463#7464#7465#7466#7467#7468#7469#747#7470#7471#7472#7473#7474#7475#7476#7477#7478#7479#748#7480#7481#7482#7483#7484#7485#7486#7487#7488#7489#749#7490#7491#7492#7493#7494#7495#7496#7497#7498#7499#75#750#7500#7501#7502#7503#7504#7505#7506#7507#7508#7509#751#7510#7511#7512#7513#7514#7515#7516#7517#7518#7519#752#7520#7521#7522#7523#7524#7525#7526#7527#7528#7529#753#7530#7531#7532#7533#7534#7535#7536#7537#7538#7539#754#7540#7541#7542#7543#7544#7545#7546#7547#7548#7549#755#7550#7551#7552#7553#7554#7555#7556#7557#7558#7559#756#7560#7561#7562#7563#7564#7565#7566#7567#7568#7569#757#7570#7571#7572#7573#7574#7575#7576#7577#7578#7579#758#7580#7581#7582#7583#7584#7585#7586#7587#7588#7589#759#7590#7591#7592#7593#7594#7595#7596#7597#7598#7599#76#760#7600#7601#7602#7603#7604#7605#7606#7607#7608#7609#761#7610#7611#7612#7613#7614#7615#7616#7617#7618#7619#762#7620#7621#7622#7623#7624#7625#7626#7627#7628#7629#763#7630#7631#7632#7633#7634#7635#7636#7637#7638#7639#764#7640#7641#7642#7643#7644#7645#7646#7647#7648#7649#765#7650#7651#7652#8766#8767#8768#8769#877#8770#8771#8772#8773#8774#8775#8776#8777#8778#8779#878#8780#8781#8782#8783#8784#8785#8786#8787#8788#8789#879#8790#8791#8792#8793#8794#8795#8796#8797#8798#8799#88#880#8800#8801#8802#8803#8804#8805#8806#8807#8808#8809#881#8810#8811#8812#8813#8814#8815#8816#8817#8818#8819#882#8820#8821#8822#8823#8824#8825#8826#8827#8828#8829#883#8830#8831#8832#8833#8834#8835#8836#8837#8838#8839#884#8840#8841#8842#8843#8844#8845#8846#8847#8848#8849#885#8850#8851#8852#8853#8854#8855#8856#8857#8858#8859#886#8860#8861#8862#8863#8864#8865#8866#8867#8868#8869#887#8870#8871#8872#8873#8874#8875#8876#8877#8878#8879#888#8880#8881#8882#8883#8884#8885#8886#8887#8888#8889#889#8890#8891#8892#8893#8894#8895#8896#8897#8898#8899#89#890#8900#8901#8902#8903#8904#8905#8906#8907#8908#8909#891#8910#8911#8912#8913#8914#8915#8916#8917#8918#8919#892#8920#8921#8922#8923#8924#8925#8926#8927#8928#8929#893#8930#8931#8932#8933#8934#8935#8936#8937#8938#8939#894#8940#8941#8942#8943#8944#8945#8946#8947#8948#8949#895#8950#8951#8952#8953#8954#8955#8956#8957#8958#8959#896#8960#8961#8962#8963#8964#8965#8966#8967#8968#8969#897#8970#8971#8972#8973#8974#8975#8976#8977#8978#8979#898#8980#8981#8982#8983#8984#8985#8986#8987#8988#8989#899#8990#8991#8992#8993#8994#8995#8996#8997#8998#8999#9#90#900#9000#9001#9002#9003#9004#9005#9006#9007#9008#9009#901#9010#9011#9012#9013#9014#9015#9016#9017#9018#9019#902#9020#9021#9022#9023#9024#9025#9026#9027#9028#9029#903#9030#9031#9032#9033#9034#9035#9036#9037#9038#9039#904#9040#9041#9042#9043#9044#9045#9046#9047#9048#9049#905#9050#9051#9052#9053#9054#9055#9056#9057#9058#9059#906#9060#9061#9062#9063#9064#9065#9066#9067#9068#9069#907#9070#9071#9072#9073#9074#9075#9076#9077#9078#9079#908#9080#9081#9082#9083#9084#9085#9086#9087#9088#9089#909#9090#91#910#911#912#913#914#915#916#917#918#919#92#920#921#922#923#924#925#926#927#928#929#93#930#931#932#933#934#935#936#937#938#939#94#940#941#942#943#944#945#946#947#948#949#95#950#951#952#953#954#955#956#957#958#959#96#960#961#962#963#964#965#966#967#968#969#97#970#971#972#973#974#975#976#977#978#979#98#980#981#982#983#984#985#986#987#988#989#99#990#991#992#993#994#995#996#997#998#999";
        let keys = output
            .to_string()
            .split_inclusive('\n')
            .map(|s| {
                let s = s.to_string();
                s.trim_end().to_string()
            })
            .collect::<Vec<_>>();

        let mut tables = vec![];
        for key in keys {
            let keys = key
                .split("#")
                .map(|key| key.as_bytes().to_vec())
                .collect::<Vec<_>>();
            tables.push(keys);
        }
        tables
    }
}
