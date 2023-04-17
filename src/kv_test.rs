use log::info;
use log::kv::ToValue;
use std::env::temp_dir;
use std::io::Write;
use std::process::id;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::iterator::IteratorOptions;
use crate::types::XArc;
use crate::value_log::Entry;
use crate::{kv::KV, options::Options, Error};

fn get_test_option(dir: &str) -> Options {
    let mut opt = Options::default();
    opt.max_table_size = 1 << 15; //Force more compaction.
    opt.level_one_size = 4 << 15; // Force more compaction.
    opt.dir = Box::new(dir.clone().to_string());
    opt.value_dir = Box::new(dir.to_string());
    opt
}

#[tokio::test]
async fn t_1_write() {
    use crate::test_util::{mock_log, mock_log_terminal, random_tmp_dir, tracing_log};
    tracing_log();
    // console_subscriber::init();
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
        info!(
            "KVItem : {:?}, value: {:?}",
            item.read().await.vptr(),
            b"word"
        );
        let item = item.read().await;
        assert_eq!(item.key(), format!("{}", i).as_bytes());
        assert_eq!(item.get_value().await.unwrap(), b"word".to_vec());
        i += 1;
    }
}

#[tokio::test]
async fn t_cas() {
    let n = 400;
    let kv = build_kv().await;
    // console_subscriber::init();
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
        if cc == 5 {
            cc = 6;
        } else {
            cc = 5;
        }
        let ret = kv.compare_and_set(key, value, cc).await.unwrap_err();
        assert_eq!(ret.to_string(), Error::ValueCasMisMatch.to_string());
    }

    for i in 0..n {
        let key = format!("{}", i).into_bytes();
        let value = format!("zzz{}", i).into_bytes();
        let ret = kv
            .compare_and_set(key, value, items[i].read().await.counter())
            .await;
        assert!(ret.is_ok());
    }

    for i in 0..n {
        let key = format!("{}", i).as_bytes().to_vec();
        let value = format!("zzz{}", i).as_bytes().to_vec();
        let got = kv.get(&key).await.unwrap();
        assert_eq!(got, value);
    }
}

#[tokio::test]
async fn t_kv_get() {
    let kv = build_kv().await;
    kv.set(b"key1".to_vec(), b"value1".to_vec(), 0x08)
        .await
        .unwrap();
    let got = kv.get_with_ext(b"key1").await.unwrap();
    assert_eq!(
        got.read().await.get_value().await.unwrap(),
        b"value1".to_vec()
    );
    assert_eq!(got.read().await.user_meta(), 0x08);
    assert!(got.read().await.counter() > 0);

    kv.set(b"key1".to_vec(), b"val2".to_vec(), 0x09)
        .await
        .unwrap();
    let got = kv.get_with_ext(b"key1").await.unwrap();
    assert_eq!(
        got.read().await.get_value().await.unwrap(),
        b"val2".to_vec()
    );
    assert_eq!(got.read().await.user_meta(), 0x09);
    assert!(got.read().await.counter() > 0);

    kv.delete(b"key1").await.unwrap();
    let got = kv.get_with_ext(b"key1").await;
    assert!(got.is_err());

    kv.set(b"key1".to_vec(), b"val3".to_vec(), 0x01)
        .await
        .unwrap();
    let got = kv.get_with_ext(b"key1").await.unwrap();
    assert_eq!(
        got.read().await.get_value().await.unwrap(),
        b"val3".to_vec()
    );
    assert_eq!(got.read().await.user_meta(), 0x01);
    assert!(got.read().await.counter() > 0);

    let long = vec![1u8; 1 << 10];
    kv.set(b"key1".to_vec(), long.clone(), 0x00).await.unwrap();
    let got = kv.get_with_ext(b"key1").await.unwrap();
    assert_eq!(got.read().await.get_value().await.unwrap(), long);
    assert_eq!(got.read().await.user_meta(), 0x00);
    assert!(got.read().await.counter() > 0);
}

#[tokio::test]
async fn t_exists() {
    let kv = build_kv().await;
    // populate with one entry
    kv.set(b"key1".to_vec(), b"val1".to_vec(), 0x00)
        .await
        .unwrap();
    let tests = vec![
        (b"key1".to_vec(), true, " valid key"),
        (b"key2".to_vec(), false, "non exist key"),
    ];
    for (idx, tt) in tests.into_iter().enumerate() {
        let exists = kv.exists(&tt.0).await.unwrap();
        assert_eq!(exists, tt.1, "{}", idx);
    }
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
#[tokio::test]
async fn t_get_more() {
    let kv = build_kv().await;
    let n = 10000;
    let m = 100;
    for i in (0..n).step_by(m) {
        if i % 10000 == 0 {
            info!("Put i={}", i);
        }
        let mut entries = vec![];
        for j in i..(i + m) {
            if j >= n {
                break;
            }
            entries.push(
                Entry::default()
                    .key(format!("{}", j).into_bytes())
                    .value(format!("{}", j).into_bytes()),
            );
        }
        let ret = kv.batch_set(entries).await;
        for e in ret {
            assert!(e.is_ok(), "entry with error: {}", e.unwrap_err());
        }
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
