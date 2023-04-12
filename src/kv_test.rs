use log::info;
use log::kv::ToValue;
use std::env::temp_dir;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::iterator::IteratorOptions;
use crate::types::XArc;
use crate::value_log::Entry;
use crate::{kv::KV, options::Options};

fn get_test_option(dir: &str) -> Options {
    let mut opt = Options::default();
    opt.max_table_size = 1 << 15; //Force more compaction.
    opt.level_one_size = 4 << 15; // Force more compaction.
    opt.dir = Box::new(dir.clone().to_string());
    opt.value_dir = Box::new(dir.to_string());
    opt
}

#[tokio::test]
async fn t_write() {
    use crate::test_util::{mock_log, mock_log_terminal, random_tmp_dir, tracing_log};
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    let res = kv.set(b"hello".to_vec(), b"word".to_vec(), 10).await;
    assert!(res.is_ok());
    let got = kv._get(b"hello");
    assert!(got.is_ok());
    assert_eq!(&got.unwrap().value, b"word");
}

#[tokio::test]
async fn t_batch_write() {
    use crate::test_util::{mock_log, mock_log_terminal, random_tmp_dir, tracing_log};
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    let n = 100;
    for i in 0..n {
        let res = kv
            .set(format!("{}", i).as_bytes().to_vec(), b"word".to_vec(), 10)
            .await;
        assert!(res.is_ok());
    }

    for i in 0..n {
        let got = kv._get(format!("{}", i).as_bytes());
        assert!(got.is_ok());
        assert_eq!(&got.unwrap().value, b"word");
    }
}

#[tokio::test]
async fn t_concurrent_write() {
    use crate::test_util::{mock_log, mock_log_terminal, random_tmp_dir, tracing_log};
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    let mut wg = awaitgroup::WaitGroup::new();
    let n = 200;
    for i in 0..n {
        let kv = kv.clone();
        let wk = wg.worker();
        tokio::spawn(async move {
            let res = kv
                .set(format!("{}", i).as_bytes().to_vec(), b"word".to_vec(), 10)
                .await;
            assert!(res.is_ok());
            wk.done();
        });
    }

    wg.wait().await;
    info!("Starting iteration");
    let itr = kv
        .new_iterator(IteratorOptions {
            reverse: false,
            pre_fetch_values: true,
            pre_fetch_size: 10,
        })
        .await;
    let mut i = 0;
    while let Some(item) = itr.next().await {
        let item = item.read().await;
        assert_eq!(item.key(), format!("{}", i).as_bytes());
        assert_eq!(item.get_value().await.unwrap(), b"word".to_vec());
        i += 1;
    }
}

#[tokio::test]
async fn t_cas() {
    let n = 100;
    let kv = build_kv().await;
    let entries = (0..n)
        .into_iter()
        .map(|i| {
            Entry::default()
                .key(format!("{}", i).into_bytes())
                .value(format!("{}", i).into_bytes())
        })
        .collect::<Vec<_>>();
    for got in kv.batch_set(entries.clone()).await {
        assert!(got.is_ok());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut items = vec![];
    for i in 0..n {
        let key = format!("{}", i).as_bytes().to_vec();
        let value = format!("{}", i).as_bytes().to_vec();
        let got = kv.get_with_ext(&key).await.unwrap();
        let got_value = got.read().await.get_value().await.unwrap();
        assert_eq!(got_value, value);
        items.push(got);
    }

    for i in 0..n {
        let key = format!("{}", i).into_bytes();
        let value = format!("{}", i).into_bytes();
        let mut cc = items[i].read().await.counter();
        println!("counter: {}", cc);
        if cc == 5 {
            cc = 6;
        } else {
            cc = 5;
        }
        assert!(kv.compare_and_set(key, value, cc).await.is_err());
    }
}

async fn build_kv() -> XArc<KV> {
    use crate::test_util::{mock_log, mock_log_terminal, random_tmp_dir, tracing_log};
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    kv
}
