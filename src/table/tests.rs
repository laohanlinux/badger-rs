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
    use core::panic;
    use log::debug;
    use memmap::MmapOptions;
    use rand::random;
    use serde_json::ser::CharEscape::Tab;
    use std::borrow::Borrow;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::collections::{HashMap, HashSet};
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
    use tracing::info;

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
        // TODO maybe next
        let item = miter.rewind().unwrap();
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
        assert_eq!(mitr.rewind().unwrap().key(), b"k1");
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

    // #[test]
    // fn chaos_merge_iterator_ext() {
    //     crate::test_util::tracing_log();
    //     let keys = keypairs("test_data/merge_iterator_ext.txt");
    //     let tables = keys
    //         .into_iter()
    //         .map(|kp| TableBuilder::new().item_keypair(kp).build_by_iterator())
    //         .collect::<Vec<_>>();
    //     tracing_log::log::info!("tables count {}", tables.len());
    //     let mut itr: Vec<Box<dyn Xiterator<Output = IteratorItem>>> = vec![];
    //     let mut num = 0;
    //     let mut all = HashMap::new();
    //     let mut last_one = b"9999999999999999999999999".to_vec();
    //     for (index, table) in tables.into_iter().enumerate() {
    //         let iter = IteratorImpl::new(table, false);
    //         iter.rewind();
    //         let mut has = HashSet::new();
    //         while let Some(item) = iter.peek() {
    //             if item.key() < last_one.as_slice() {
    //                 last_one = item.key.clone();
    //             }
    //             let ok = has.insert(hex_str(item.key()));
    //             assert!(ok, "dump key, index: {}, {}", index, hex_str(item.key()));
    //             // tracing_log::log::info!(
    //             //     "{}=>{}. {}",
    //             //     num,
    //             //     hex_str(item.key()),
    //             //     item.value().pretty()
    //             // );
    //             if let Some(old) = all.insert(hex_str(item.key()), item.clone()) {
    //                 tracing_log::log::warn!(
    //                     "{} has inserted, old: {} => new: {}",
    //                     hex_str(item.key()),
    //                     old.value().pretty(),
    //                     item.value().pretty(),
    //                 );
    //             }
    //             num += 1;
    //             iter.next();
    //         }
    //         itr.push(Box::new(iter));
    //     }
    //     log::info!("total:{}, distinct:{}", num, all.len());
    // }

    #[test]
    fn chaos_merge_iterator() {
        crate::test_util::tracing_log();
        use rand::seq::SliceRandom;
        use rand::thread_rng;
        let mut keys = keys();
        for key in keys.clone() {
            let count = key.len();
            let key = key.into_iter();
            let st = key.collect::<HashSet<Vec<_>>>();
            assert_eq!(st.len(), count);
        }
        //keys.shuffle(&mut thread_rng());
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
        let mut num = 0;
        let mut all = vec![];
        for table in tables {
            let iter = IteratorImpl::new(table, false);
            while let Some(item) = iter.next() {
                log::info!("{}=>{}", num, hex_str(item.key()));
                all.push(hex_str(item.key()));
                num += 1;
            }
            itr.push(Box::new(iter));
        }
        assert_eq!(count, num);

        let mut mitr = MergeIterOverBuilder::default().add_batch(itr).build();
        mitr.rewind();
        let mut _count = 0;
        while let Some(item) = mitr.peek() {
            mitr.next();
            _count += 1;
        }

        all.sort();
        all.iter().for_each(|key| log::info!("{}", key));
        //assert_eq!(count, _count);
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

    fn build_table2(mut key_value: Vec<IteratorItem>) -> (File, String) {
        let mut builder = Builder::default();
        let dir = temp_dir().join(random::<u64>().to_string() + FILE_SUFFIX);
        let file_name = dir.to_str().unwrap();
        let mut fp = open_synced_file(file_name, true).unwrap();
        let mut has = HashSet::new();
        for (i, item) in key_value.iter().enumerate() {
            assert!(has.insert(item.key().clone()));
            let got = builder.add(item.key(), item.value());
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
        item_kp: Vec<IteratorItem>,
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
                item_kp: Vec::new(),
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

        fn item_keypair(mut self, kp: Vec<IteratorItem>) -> Self {
            self.item_kp = kp;
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

        fn build_by_iterator(&mut self) -> Table {
            let table = build_table2(self.item_kp.clone());
            self.path = table.1.clone();
            let t1 = TableCore::open_table(table.0, self.path.as_ref(), FileLoadingMode::MemoryMap)
                .unwrap();
            Table::new(t1)
        }
    }

    fn keys() -> Vec<Vec<Vec<u8>>> {
        let output = "1000#1001#1002#1003#1004#1005#1006#1007#1008#1009#1010#1011#1012#1013#1014#1015#1016#1017#1018#1019#1020#1021#1022#1023#1024#1025#1026#1027#1028#1029#1030#1031#1032#1033#1034#1035#1036#1037#1038#1039#1040#1041#1042#1043#1044#1045#1046#1047#1048#1049#1050#1051#1052#1053#1054#1055#1056#1057#1058#1059#1060#1061#1062#1063#1064#1065#1066#1067#1068#1069#1070#1071#1072#1073#1074#1075#1076#1077#1078#1079#1080#1081#1082#1083#1084#1085#1086#1087#1088#1089#1090#1091#1092#1093#1094#1095#1096#1097#1098#1099#800#801#802#803#804#805#806#807#808#809#810#811#812#813#814#815#816#817#818#819#820#821#822#823#824#825#826#827#828#829#830#831#832#833#834#835#836#837#838#839#840#841#842#843#844#845#846#847#848#849#850#851#852#853#854#855#856#857#858#859#860#861#862#863#864#865#866#867#868#869#870#871#872#873#874#875#876#877#878#879#880#881#882#883#884#885#886#887#888#889#890#891#892#893#894#895#896#897#898#899#900#901#902#903#904#905#906#907#908#909#910#911#912#913#914#915#916#917#918#919#920#921#922#923#924#925#926#927#928#929#930#931#932#933#934#935#936#937#938#939#940#941#942#943#944#945#946#947#948#949#950#951#952#953#954#955#956#957#958#959#960#961#962#963#964#965#966#967#968#969#970#971#972#973#974#975#976#977#978#979#980#981#982#983#984#985#986#987#988#989#990#991#992#993#994#995#996#997#998#999
1100#1101#1102#1103#1104#1105#1106#1107#1108#1109#1110#1111#1112#1113#1114#1115#1116#1117#1118#1119#1120#1121#1122#1123#1124#1125#1126#1127#1128#1129#1130#1131#1132#1133#1134#1135#1136#1137#1138#1139#1140#1141#1142#1143#1144#1145#1146#1147#1148#1149#1150#1151#1152#1153#1154#1155#1156#1157#1158#1159#1160#1161#1162#1163#1164#1165#1166#1167#1168#1169#1170#1171#1172#1173#1174#1175#1176#1177#1178#1179#1180#1181#1182#1183#1184#1185#1186#1187#1188#1189#1190#1191#1192#1193#1194#1195#1196#1197#1198#1199#1200#1201#1202#1203#1204#1205#1206#1207#1208#1209#1210#1211#1212#1213#1214#1215#1216#1217#1218#1219#1220#1221#1222#1223#1224#1225#1226#1227#1228#1229#1230#1231#1232#1233#1234#1235#1236#1237#1238#1239#1240#1241#1242#1243#1244#1245#1246#1247#1248#1249#1250#1251#1252#1253#1254#1255#1256#1257#1258#1259#1260#1261#1262#1263#1264#1265#1266#1267#1268#1269#1270#1271#1272#1273#1274#1275#1276#1277#1278#1279#1280#1281#1282#1283#1284#1285#1286#1287#1288#1289#1290#1291#1292#1293#1294#1295#1296#1297#1298#1299#1300#1301#1302#1303#1304#1305#1306#1307#1308#1309#1310#1311#1312#1313#1314#1315#1316#1317#1318#1319#1320#1321#1322#1323#1324#1325#1326#1327#1328#1329#1330#1331#1332#1333#1334#1335#1336#1337#1338#1339#1340#1341#1342#1343#1344#1345#1346#1347#1348#1349#1350#1351#1352#1353#1354#1355#1356#1357#1358#1359#1360#1361#1362#1363#1364#1365#1366#1367#1368#1369#1370#1371#1372#1373#1374#1375#1376#1377#1378#1379#1380#1381#1382#1383#1384#1385#1386#1387#1388#1389#1390#1391#1392#1393#1394#1395#1396#1397#1398#1399
1400#1401#1402#1403#1404#1405#1406#1407#1408#1409#1410#1411#1412#1413#1414#1415#1416#1417#1418#1419#1420#1421#1422#1423#1424#1425#1426#1427#1428#1429#1430#1431#1432#1433#1434#1435#1436#1437#1438#1439#1440#1441#1442#1443#1444#1445#1446#1447#1448#1449#1450#1451#1452#1453#1454#1455#1456#1457#1458#1459#1460#1461#1462#1463#1464#1465#1466#1467#1468#1469#1470#1471#1472#1473#1474#1475#1476#1477#1478#1479#1480#1481#1482#1483#1484#1485#1486#1487#1488#1489#1490#1491#1492#1493#1494#1495#1496#1497#1498#1499#1500#1501#1502#1503#1504#1505#1506#1507#1508#1509#1510#1511#1512#1513#1514#1515#1516#1517#1518#1519#1520#1521#1522#1523#1524#1525#1526#1527#1528#1529#1530#1531#1532#1533#1534#1535#1536#1537#1538#1539#1540#1541#1542#1543#1544#1545#1546#1547#1548#1549#1550#1551#1552#1553#1554#1555#1556#1557#1558#1559#1560#1561#1562#1563#1564#1565#1566#1567#1568#1569#1570#1571#1572#1573#1574#1575#1576#1577#1578#1579#1580#1581#1582#1583#1584#1585#1586#1587#1588#1589#1590#1591#1592#1593#1594#1595#1596#1597#1598#1599#1600#1601#1602#1603#1604#1605#1606#1607#1608#1609#1610#1611#1612#1613#1614#1615#1616#1617#1618#1619#1620#1621#1622#1623#1624#1625#1626#1627#1628#1629#1630#1631#1632#1633#1634#1635#1636#1637#1638#1639#1640#1641#1642#1643#1644#1645#1646#1647#1648#1649#1650#1651#1652#1653#1654#1655#1656#1657#1658#1659#1660#1661#1662#1663#1664#1665#1666#1667#1668#1669#1670#1671#1672#1673#1674#1675#1676#1677#1678#1679#1680#1681#1682#1683#1684#1685#1686#1687#1688#1689#1690#1691#1692#1693#1694#1695#1696#1697#1698#1699
1700#1701#1702#1703#1704#1705#1706#1707#1708#1709#1710#1711#1712#1713#1714#1715#1716#1717#1718#1719#1720#1721#1722#1723#1724#1725#1726#1727#1728#1729#1730#1731#1732#1733#1734#1735#1736#1737#1738#1739#1740#1741#1742#1743#1744#1745#1746#1747#1748#1749#1750#1751#1752#1753#1754#1755#1756#1757#1758#1759#1760#1761#1762#1763#1764#1765#1766#1767#1768#1769#1770#1771#1772#1773#1774#1775#1776#1777#1778#1779#1780#1781#1782#1783#1784#1785#1786#1787#1788#1789#1790#1791#1792#1793#1794#1795#1796#1797#1798#1799#1800#1801#1802#1803#1804#1805#1806#1807#1808#1809#1810#1811#1812#1813#1814#1815#1816#1817#1818#1819#1820#1821#1822#1823#1824#1825#1826#1827#1828#1829#1830#1831#1832#1833#1834#1835#1836#1837#1838#1839#1840#1841#1842#1843#1844#1845#1846#1847#1848#1849#1850#1851#1852#1853#1854#1855#1856#1857#1858#1859#1860#1861#1862#1863#1864#1865#1866#1867#1868#1869#1870#1871#1872#1873#1874#1875#1876#1877#1878#1879#1880#1881#1882#1883#1884#1885#1886#1887#1888#1889#1890#1891#1892#1893#1894#1895#1896#1897#1898#1899#1900#1901#1902#1903#1904#1905#1906#1907#1908#1909#1910#1911#1912#1913#1914#1915#1916#1917#1918#1919#1920#1921#1922#1923#1924#1925#1926#1927#1928#1929#1930#1931#1932#1933#1934#1935#1936#1937#1938#1939#1940#1941#1942#1943#1944#1945#1946#1947#1948#1949#1950#1951#1952#1953#1954#1955#1956#1957#1958#1959#1960#1961#1962#1963#1964#1965#1966#1967#1968#1969#1970#1971#1972#1973#1974#1975#1976#1977#1978#1979#1980#1981#1982#1983#1984#1985#1986#1987#1988#1989#1990#1991#1992#1993#1994#1995#1996#1997#1998#1999
1500#1501#1502#1503#1504#1505#1506#1507#1508#1509#1510#1511#1512#1513#1514#1515#1516#1517#1518#1519#1520#1521#1522#1523#1524#1525#1526#1527#1528#1529#1530#1531#1532#1533#1534#1535#1536#1537#1538#1539#1540#1541#1542#1543#1544#1545#1546#1547#1548#1549#1550#1551#1552#1553#1554#1555#1556#1557#1558#1559#1560#1561#1562#1563#1564#1565#1566#1567#1568#1569#1570#1571#1572#1573#1574#1575#1576#1577#1578#1579#1580#1581#1582#1583#1584#1585#1586#1587#1588#1589#1590#1591#1592#1593#1594#1595#1596#1597#1598#1599#1600#1601#1602#1603#1604#1605#1606#1607#1608#1609#1610#1611#1612#1613#1614#1615#1616#1617#1618#1619#1620#1621#1622#1623#1624#1625#1626#1627#1628#1629#1630#1631#1632#1633#1634#1635#1636#1637#1638#1639#1640#1641#1642#1643#1644#1645#1646#1647#1648#1649#1650#1651#1652#1653#1654#1655#1656#1657#1658#1659#1660#1661#1662#1663#1664#1665#1666#1667#1668#1669#1670#1671#1672#1673#1674#1675#1676#1677#1678#1679#1680#1681#1682#1683#1684#1685#1686#1687#1688#1689#1690#1691#1692#1693#1694#1695#1696#1697#1698#1699#1700#1701#1702#1703#1704#1705#1706#1707#1708#1709#1710#1711#1712#1713#1714#1715#1716#1717#1718#1719#1720#1721#1722#1723#1724#1725#1726#1727#1728#1729#1730#1731#1732#1733#1734#1735#1736#1737#1738#1739#1740#1741#1742#1743#1744#1745#1746#1747#1748#1749#1750#1751#1752#1753#1754#1755#1756#1757#1758#1759#1760#1761#1762#1763#1764#1765#1766#1767#1768#1769#1770#1771#1772#1773#1774#1775#1776#1777#1778#1779#1780#1781#1782#1783#1784#1785#1786#1787#1788#1789#1790#1791#1792#1793#1794#1795#1796#1797#1798#1799
1200#1201#1202#1203#1204#1205#1206#1207#1208#1209#1210#1211#1212#1213#1214#1215#1216#1217#1218#1219#1220#1221#1222#1223#1224#1225#1226#1227#1228#1229#1230#1231#1232#1233#1234#1235#1236#1237#1238#1239#1240#1241#1242#1243#1244#1245#1246#1247#1248#1249#1250#1251#1252#1253#1254#1255#1256#1257#1258#1259#1260#1261#1262#1263#1264#1265#1266#1267#1268#1269#1270#1271#1272#1273#1274#1275#1276#1277#1278#1279#1280#1281#1282#1283#1284#1285#1286#1287#1288#1289#1290#1291#1292#1293#1294#1295#1296#1297#1298#1299#1300#1301#1302#1303#1304#1305#1306#1307#1308#1309#1310#1311#1312#1313#1314#1315#1316#1317#1318#1319#1320#1321#1322#1323#1324#1325#1326#1327#1328#1329#1330#1331#1332#1333#1334#1335#1336#1337#1338#1339#1340#1341#1342#1343#1344#1345#1346#1347#1348#1349#1350#1351#1352#1353#1354#1355#1356#1357#1358#1359#1360#1361#1362#1363#1364#1365#1366#1367#1368#1369#1370#1371#1372#1373#1374#1375#1376#1377#1378#1379#1380#1381#1382#1383#1384#1385#1386#1387#1388#1389#1390#1391#1392#1393#1394#1395#1396#1397#1398#1399#1400#1401#1402#1403#1404#1405#1406#1407#1408#1409#1410#1411#1412#1413#1414#1415#1416#1417#1418#1419#1420#1421#1422#1423#1424#1425#1426#1427#1428#1429#1430#1431#1432#1433#1434#1435#1436#1437#1438#1439#1440#1441#1442#1443#1444#1445#1446#1447#1448#1449#1450#1451#1452#1453#1454#1455#1456#1457#1458#1459#1460#1461#1462#1463#1464#1465#1466#1467#1468#1469#1470#1471#1472#1473#1474#1475#1476#1477#1478#1479#1480#1481#1482#1483#1484#1485#1486#1487#1488#1489#1490#1491#1492#1493#1494#1495#1496#1497#1498#1499
1000#1001#1002#1003#1004#1005#1006#1007#1008#1009#1010#1011#1012#1013#1014#1015#1016#1017#1018#1019#1020#1021#1022#1023#1024#1025#1026#1027#1028#1029#1030#1031#1032#1033#1034#1035#1036#1037#1038#1039#1040#1041#1042#1043#1044#1045#1046#1047#1048#1049#1050#1051#1052#1053#1054#1055#1056#1057#1058#1059#1060#1061#1062#1063#1064#1065#1066#1067#1068#1069#1070#1071#1072#1073#1074#1075#1076#1077#1078#1079#1080#1081#1082#1083#1084#1085#1086#1087#1088#1089#1090#1091#1092#1093#1094#1095#1096#1097#1098#1099#1100#1101#1102#1103#1104#1105#1106#1107#1108#1109#1110#1111#1112#1113#1114#1115#1116#1117#1118#1119#1120#1121#1122#1123#1124#1125#1126#1127#1128#1129#1130#1131#1132#1133#1134#1135#1136#1137#1138#1139#1140#1141#1142#1143#1144#1145#1146#1147#1148#1149#1150#1151#1152#1153#1154#1155#1156#1157#1158#1159#1160#1161#1162#1163#1164#1165#1166#1167#1168#1169#1170#1171#1172#1173#1174#1175#1176#1177#1178#1179#1180#1181#1182#1183#1184#1185#1186#1187#1188#1189#1190#1191#1192#1193#1194#1195#1196#1197#1198#1199#900#901#902#903#904#905#906#907#908#909#910#911#912#913#914#915#916#917#918#919#920#921#922#923#924#925#926#927#928#929#930#931#932#933#934#935#936#937#938#939#940#941#942#943#944#945#946#947#948#949#950#951#952#953#954#955#956#957#958#959#960#961#962#963#964#965#966#967#968#969#970#971#972#973#974#975#976#977#978#979#980#981#982#983#984#985#986#987#988#989#990#991#992#993#994#995#996#997#998#999
600#601#602#603#604#605#606#607#608#609#610#611#612#613#614#615#616#617#618#619#620#621#622#623#624#625#626#627#628#629#630#631#632#633#634#635#636#637#638#639#640#641#642#643#644#645#646#647#648#649#650#651#652#653#654#655#656#657#658#659#660#661#662#663#664#665#666#667#668#669#670#671#672#673#674#675#676#677#678#679#680#681#682#683#684#685#686#687#688#689#690#691#692#693#694#695#696#697#698#699#700#701#702#703#704#705#706#707#708#709#710#711#712#713#714#715#716#717#718#719#720#721#722#723#724#725#726#727#728#729#730#731#732#733#734#735#736#737#738#739#740#741#742#743#744#745#746#747#748#749#750#751#752#753#754#755#756#757#758#759#760#761#762#763#764#765#766#767#768#769#770#771#772#773#774#775#776#777#778#779#780#781#782#783#784#785#786#787#788#789#790#791#792#793#794#795#796#797#798#799#800#801#802#803#804#805#806#807#808#809#810#811#812#813#814#815#816#817#818#819#820#821#822#823#824#825#826#827#828#829#830#831#832#833#834#835#836#837#838#839#840#841#842#843#844#845#846#847#848#849#850#851#852#853#854#855#856#857#858#859#860#861#862#863#864#865#866#867#868#869#870#871#872#873#874#875#876#877#878#879#880#881#882#883#884#885#886#887#888#889#890#891#892#893#894#895#896#897#898#899
300#301#302#303#304#305#306#307#308#309#310#311#312#313#314#315#316#317#318#319#320#321#322#323#324#325#326#327#328#329#330#331#332#333#334#335#336#337#338#339#340#341#342#343#344#345#346#347#348#349#350#351#352#353#354#355#356#357#358#359#360#361#362#363#364#365#366#367#368#369#370#371#372#373#374#375#376#377#378#379#380#381#382#383#384#385#386#387#388#389#390#391#392#393#394#395#396#397#398#399#400#401#402#403#404#405#406#407#408#409#410#411#412#413#414#415#416#417#418#419#420#421#422#423#424#425#426#427#428#429#430#431#432#433#434#435#436#437#438#439#440#441#442#443#444#445#446#447#448#449#450#451#452#453#454#455#456#457#458#459#460#461#462#463#464#465#466#467#468#469#470#471#472#473#474#475#476#477#478#479#480#481#482#483#484#485#486#487#488#489#490#491#492#493#494#495#496#497#498#499#500#501#502#503#504#505#506#507#508#509#510#511#512#513#514#515#516#517#518#519#520#521#522#523#524#525#526#527#528#529#530#531#532#533#534#535#536#537#538#539#540#541#542#543#544#545#546#547#548#549#550#551#552#553#554#555#556#557#558#559#560#561#562#563#564#565#566#567#568#569#570#571#572#573#574#575#576#577#578#579#580#581#582#583#584#585#586#587#588#589#590#591#592#593#594#595#596#597#598#599
0#1#10#100#101#102#103#104#105#106#107#108#109#11#110#111#112#113#114#115#116#117#118#119#12#120#121#122#123#124#125#126#127#128#129#13#130#131#132#133#134#135#136#137#138#139#14#140#141#142#143#144#145#146#147#148#149#15#150#151#152#153#154#155#156#157#158#159#16#160#161#162#163#164#165#166#167#168#169#17#170#171#172#173#174#175#176#177#178#179#18#180#181#182#183#184#185#186#187#188#189#19#190#191#192#193#194#195#196#197#198#199#2#20#200#201#202#203#204#205#206#207#208#209#21#210#211#212#213#214#215#216#217#218#219#22#220#221#222#223#224#225#226#227#228#229#23#230#231#232#233#234#235#236#237#238#239#24#240#241#242#243#244#245#246#247#248#249#25#250#251#252#253#254#255#256#257#258#259#26#260#261#262#263#264#265#266#267#268#269#27#270#271#272#273#274#275#276#277#278#279#28#280#281#282#283#284#285#286#287#288#289#29#290#291#292#293#294#295#296#297#298#299#3#30#31#32#33#34#35#36#37#38#39#4#40#41#42#43#44#45#46#47#48#49#5#50#51#52#53#54#55#56#57#58#59#6#60#61#62#63#64#65#66#67#68#69#7#70#71#72#73#74#75#76#77#78#79#8#80#81#82#83#84#85#86#87#88#89#9#90#91#92#93#94#95#96#97#98#99";
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

    fn keypairs(fpath: &str) -> Vec<Vec<IteratorItem>> {
        match std::fs::read_to_string(fpath) {
            Ok(content) => {
                let c = content.split_ascii_whitespace().collect::<Vec<_>>();
                c.into_iter()
                    .map(|s| serde_json::from_str::<Vec<IteratorItem>>(s).unwrap())
                    .collect::<Vec<_>>()
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                vec![]
            }
            Err(err) => panic!("{}", err),
        }
    }
}
