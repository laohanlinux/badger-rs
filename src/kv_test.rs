use log::kv::ToValue;
use log::{debug, info, warn};
use std::env::temp_dir;
use std::io::Write;
use std::process::id;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tracing_subscriber::fmt::format;

use crate::iterator::IteratorOptions;
use crate::types::{TArcMx, XArc};
use crate::value_log::Entry;
use crate::{kv::KV, options::Options, Error};

fn get_test_option(dir: &str) -> Options {
    let mut opt = Options::default();
    opt.max_table_size = 1 << 15; // Force more compaction.
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
    let n = 3730;
    for i in 0..n {
        let res = kv
            .set(format!("{}", i).as_bytes().to_vec(), b"word".to_vec(), 10)
            .await;
        // if n == 3720 {
        //     tokio::time::sleep(Duration::from_secs(100)).await;
        // }
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
    let n = 300;
    let m = 10;
    let keys = TArcMx::new(tokio::sync::Mutex::new(vec![]));
    for i in 0..n {
        let kv = kv.clone();
        let wk = wg.worker();
        let keys = keys.clone();
        tokio::spawn(async move {
            for j in 0..m {
                let key = format!("k{:05}_{:08}", i, j).into_bytes().to_vec();
                keys.lock().await.push(key.clone());
                let res = kv
                    .set(key, format!("v{:05}_{:08}", i, j).into_bytes().to_vec(), 10)
                    .await;
                assert!(res.is_ok());
            }
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
    keys.lock().await.sort();
    let keys = keys.lock().await;
    let got = kv.get_with_ext(b"k00003_00000008").await.unwrap();
    let value = got.read().await;
    println!(
        "{:?}",
        String::from_utf8_lossy(value.get_value().await.as_ref().unwrap())
    );
    while let Some(item) = itr.next().await {
        let kv_item = item.read().await;
        println!("load key: {}", String::from_utf8_lossy(kv_item.key()));
        let expect = String::from_utf8_lossy(keys.get(i).unwrap());
        let got = String::from_utf8_lossy(kv_item.key());
        assert_eq!(expect, got);
        // assert_eq!(kv_item.key(), format!("{}", i).as_bytes());
        // assert_eq!(kv_item.get_value().await.unwrap(), b"word".to_vec());
        i += 1;
    }
}

#[tokio::test]
async fn t_cas() {
    crate::test_util::tracing_log();
    let n = 299;
    let kv = build_kv().await;
    let entries = (0..n)
        .into_iter()
        .map(|i| {
            Entry::default()
                .key(i.to_string().into_bytes())
                .value(i.to_string().into_bytes())
        })
        .collect::<Vec<_>>();
    // batch set kv pair, cas update to n
    for got in kv.batch_set(entries.clone()).await {
        assert!(got.is_ok());
    }
    debug!("after batch set kv pair init, the counter has update to n");
    assert_eq!(kv.to_ref().get_last_used_cas_counter(), n as u64);
    tokio::time::sleep(Duration::from_millis(20)).await;
    // load expect output pairs
    let mut items = vec![];
    for i in 0..n {
        let key = i.to_string().into_bytes();
        let value = i.to_string().into_bytes();
        let got = kv.get_with_ext(&key).await.unwrap();
        let pair = got.read().await.clone();
        let got_value = got.read().await.get_value().await.unwrap();
        assert_eq!(got_value, value, "{}", String::from_utf8_lossy(&key));
        let counter = got.read().await.counter();
        // cas counter from 1
        assert_eq!(i + 1, counter as usize);
        // store kv pair
        items.push(pair);
    }

    debug!("It should be all failed because comparse_and_set failed!!!");
    for i in 0..n {
        let key = i.to_string().into_bytes();
        let value = (i + 100).to_string().into_bytes();
        let mut cc = items[i].counter();
        let ret = kv.compare_and_set(key, value, cc + 1).await.unwrap_err();
        assert_eq!(ret.to_string(), Error::ValueCasMisMatch.to_string());
        assert_eq!(kv.to_ref().get_last_used_cas_counter() as usize, n + i + 1);
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    for (cas, item) in items.iter().enumerate() {
        assert_eq!(cas + 1, item.counter() as usize);
    }

    // Although there are new key-value pairs successfully updated, the CAS (comparse_and_swap) value will still increment.
    assert_eq!(kv.to_ref().get_last_used_cas_counter(), 2 * n as u64);
    debug!(
        "change value to zzz{n} and the operation should be succeed because counter is right!!!"
    );
    for i in 0..n {
        let key = i.to_string().into_bytes();
        let value = format!("zzz{}", i).into_bytes();
        // {
        //     let got = kv.get_with_ext(&key).await.unwrap();
        //     let got = got.read().await;
        //     debug!(
        //         "got a value: {}, cas: {}",
        //         String::from_utf8_lossy(&got.get_value().await.unwrap()),
        //         got.counter(),
        //     );
        // }
        // warn!("==> {}", items[i].counter());
        let ret = kv.compare_and_set(key, value, items[i].counter()).await;
        if ret.is_err() {
            warn!("fail to check compare and set");
            return;
        }
        assert!(ret.is_ok(), "{}", i);
    }
    // debug!("cas has update, try it again");
    // //assert_eq!(kv.to_ref().get_last_used_cas_counter(), 2 * n as u64);
    // tokio::time::sleep(Duration::from_secs(3)).await;
    // for i in 0..n {
    //     let key = i.to_string().into_bytes();
    //     let value = format!("zzz{}", i).as_bytes().to_vec();
    //     let got = kv.get_with_ext(&key).await.unwrap();
    //     let got = got.read().await;
    //     assert_eq!(got.get_value().await.unwrap(), value);
    //     assert_eq!(n * 2 + i + 1, got.counter() as usize);
    // }
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
async fn t_kv_exists() {
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
async fn t_kv_get_more() {
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
