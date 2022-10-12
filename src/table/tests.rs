#[cfg(test)]
mod utils {
    use crate::options::FileLoadingMode;
    use crate::table::builder::Builder;
    use crate::table::iterator::BlockIterator;
    use crate::table::table::{FILE_SUFFIX, Table, TableCore};
    use crate::y::{open_synced_file, ValueStruct};
    use rand::random;
    use std::env::temp_dir;
    use std::fmt::format;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::thread::spawn;


    #[test]
    fn it_block_iterator() {
        let mut builder = new_builder("anc", 10000);
        let data = builder.finish();
        let it = BlockIterator::new(&data);
        let mut i = 0;
        while let Some(item) = it.next() {
            println!(" key: {:?}, value: {:?}", item.key(), item.value());
            i += 1;
        }
        println!("{}", i);
    }

    #[test]
    fn it_seek_to_fist() {
        let (mut fp, path) = build_test_table("key", 10);
        let table = TableCore::open_table(fp, &path, FileLoadingMode::LoadToRADM).unwrap();
        // let mut joins = vec![];
        // for n in vec![101, 199, 200, 250, 9999, 10000] {
        //     joins.push(spawn(move || {
        //         let f = build_test_table("key", n);
        //         let table = TableCore::open_table(f, FileLoadingMode::LoadToRADM).unwrap();
        //     }));
        // }
        //
        // for join in joins {
        //     join.join().unwrap();
        // }
    }

    fn build_table(mut key_value: Vec<(Vec<u8>, Vec<u8>)>) -> (File, String) {
        let mut builder = Builder::default();
        let file_name = format!("{}{}{}", temp_dir().to_str().unwrap(), random::<u64>(), FILE_SUFFIX);
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
