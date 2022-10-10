#[cfg(test)]
mod utils {
    use crate::table::builder::Builder;
    use crate::y::{open_synced_file, ValueStruct};
    use rand::random;
    use std::env::temp_dir;
    use std::fmt::format;
    use std::fs::File;
    use std::io::{Cursor, Write};


    #[test]
    fn it() {
        // let mut cousor = Cursor::new(Vec::with_capacity(10));
        // cousor.write_all(&vec![1, 2, 3]).unwrap();
        // println!("{:?}", cousor.into_inner());
        let mut fp = build_test_table("", 1000);
    }

    fn build_table(mut key_value: Vec<(Vec<u8>, Vec<u8>)>) -> File {
        let mut builder = Builder::default();
        let file_name = format!("{}{}.sst", temp_dir().to_str().unwrap(), random::<u64>());
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
        open_synced_file(&file_name, true).unwrap()
    }

    fn build_test_table(prefix: &str, n: usize) -> File {
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
