#[cfg(test)]
mod utils {
    use crate::options::FileLoadingMode;
    use crate::table::builder::Builder;
    use crate::table::iterator::BlockIterator;
    use crate::table::table::{Table, TableCore, FILE_SUFFIX};
    use crate::y::iterator::Iterator;
    use crate::y::{open_synced_file, read_at, ValueStruct};
    use memmap::MmapOptions;
    use rand::random;
    use std::env::temp_dir;
    use std::fmt::format;
    use std::fs::File;
    use std::io::{Cursor, Seek, SeekFrom, Write};
    use std::sync::Arc;
    use std::thread::spawn;

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
            let iter = crate::table::iterator::Iterator::new(&table, false);
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

        let iter = crate::table::iterator::Iterator::new(&table, false);

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

        let iter = crate::table::iterator::Iterator::new(&table, false);
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
        for n in vec![101] {
            let (mut fp, path) = build_test_table("key", n);
            let table = TableCore::open_table(fp, &path, FileLoadingMode::MemoryMap).unwrap();
            let iter = crate::table::iterator::Iterator::new(&table, false);
            iter.reset();
            let value = iter.seek(b"");
            assert!(value.is_some());
            // No need to do a Next.
            // ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
            let mut count = 0;
            while let Some(value) = iter.next() {
                let value = value.value();
                println!("{:?}", value.value);
                assert_eq!(value.value, format!("{}", count).as_bytes().to_vec());
                assert_eq!(value.meta, 'A' as u8);
                assert_eq!(value.cas_counter, count as u64);
                count += 1;
            }
            assert_eq!(count, n as isize);
        }
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

    fn build_table(mut key_value: Vec<(Vec<u8>, Vec<u8>)>) -> (File, String) {
        let mut builder = Builder::default();
        let file_name = format!(
            "{}{}{}",
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

    fn new_builder(prefix: &str, n: usize) -> Builder {
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

    fn build_test_table(prefix: &str, n: usize) -> (File, String) {
        assert!(n <= 10000);
        let mut key_values = vec![];
        for i in 0..n {
            let key = key(prefix, i).as_bytes().to_vec();
            let v = format!("{}", i).as_bytes().to_vec();
            key_values.push((key, v));
        }

        build_table(key_values)
    }

    fn key(prefix: &str, n: usize) -> String {
        format!("{}{:04}", prefix, n)
    }
}
