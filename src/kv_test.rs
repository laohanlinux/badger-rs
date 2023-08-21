use awaitgroup::WaitGroup;
use drop_cell::defer;
use itertools::Itertools;
use log::kv::ToValue;
use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::env::temp_dir;
use std::io::{Read, Write};
use std::process::id;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;
use tracing_subscriber::fmt::format;

use crate::iterator::IteratorOptions;
use crate::test_util::{push_log, remove_push_log, tracing_log};
use crate::types::{TArcMx, XArc};
use crate::value_log::{Entry, MetaBit};
use crate::y::hex_str;
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
    use crate::test_util::{random_tmp_dir, tracing_log};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t_batch_write() {
    use crate::test_util::{random_tmp_dir, tracing_log};
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    let n = 50000;
    let mut batch = vec![];
    let start = SystemTime::now();
    for i in 1..n {
        let key = i.to_string().into_bytes();
        batch.push(
            Entry::default()
                .key(key)
                .value(b"word".to_vec())
                .user_meta(10),
        );
    }
    for chunk in batch.chunks(100) {
        let res = kv.batch_set(chunk.to_vec()).await;
        for _res in res {
            assert!(_res.is_ok());
        }
    }
    batch.clear();

    {
        let lc = kv.must_lc();
        assert!(lc.validate().is_ok());
    }
    warn!(
        "after push first, cost {}ms",
        SystemTime::now().duration_since(start).unwrap().as_millis()
    );
    for i in 1..n {
        let key = i.to_string().into_bytes();
        let got = kv.exists(&key).await;
        assert!(got.is_ok() && got.unwrap(), "#{}", hex_str(&key));
    }
    let found = kv.exists(b"non-exists").await;
    assert!(found.is_ok());
    assert!(!found.unwrap());

    warn!(
        "after check exist, cost {}ms",
        SystemTime::now().duration_since(start).unwrap().as_millis()
    );

    for i in 1..n {
        let key = i.to_string().into_bytes();
        batch.push(
            Entry::default()
                .key(key)
                .value(b"word".to_vec())
                .user_meta(10)
                .meta(MetaBit::BIT_DELETE.bits()),
        );
    }
    batch.reverse();
    for chunk in batch.chunks(100) {
        let res = kv.batch_set(chunk.to_vec()).await;
        for _res in res {
            assert!(_res.is_ok());
        }
    }
    batch.clear();

    {
        let lc = kv.must_lc();
        assert!(lc.validate().is_ok());
    }
    warn!(
        "after push2 {}s",
        SystemTime::now().duration_since(start).unwrap().as_secs()
    );
    for i in 1..n {
        let key = i.to_string().into_bytes();
        let got = kv.exists(&key).await;
        assert!(got.is_ok() && !got.unwrap(), "#{}", hex_str(&key));
    }
    warn!(
        "cost time: {}s",
        SystemTime::now().duration_since(start).unwrap().as_secs()
    );
    kv.must_lc().print_level_fids();
}

#[tokio::test]
async fn t_concurrent_write() {
    use crate::test_util::{random_tmp_dir, tracing_log};
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    let mut wg = awaitgroup::WaitGroup::new();
    let n = 20;
    let m = 500;
    let keys = TArcMx::new(tokio::sync::Mutex::new(vec![]));
    for i in 0..n {
        let kv = kv.clone();
        let wk = wg.worker();
        let keys = keys.clone();
        tokio::spawn(async move {
            defer! {wk.done()}
            for j in 0..m {
                let key = format!("k{:05}_{:08}", i, j).into_bytes().to_vec();
                keys.lock().await.push(key.clone());
                let res = kv
                    .set(key, format!("v{:05}_{:08}", i, j).into_bytes().to_vec(), 10)
                    .await;
                assert!(res.is_ok());
            }
        });
    }

    wg.wait().await;
    info!("Starting iteration");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut keys = keys.lock().await;
    keys.sort();
    for key in keys.iter() {
        assert!(kv.exists(key).await.unwrap());
    }

    let mut count_set = HashSet::new();
    let itr = kv
        .new_iterator(IteratorOptions {
            reverse: false,
            pre_fetch_values: true,
            pre_fetch_size: 10,
        })
        .await;

    assert!(itr.peek().await.is_none());
    itr.rewind().await;
    let mut i = 0;
    while let Some(item) = itr.peek().await {
        let kv_item = item.read().await;
        count_set.insert(hex_str(kv_item.key().clone()));
        let expect = String::from_utf8_lossy(keys.get(i).unwrap());
        let got = String::from_utf8_lossy(kv_item.key());
        assert_eq!(expect, got);
        i += 1;
        itr.next().await;
    }

    for key in keys.iter() {
        assert!(count_set.contains(&hex_str(key)));
    }

    itr.close().await.expect("");
}

#[tokio::test]
async fn t_kv_cas() {
    tracing_log();
    let n = 299;
    let kv = build_kv().await;
    let opt = kv.opt.clone();
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
        let entry = Entry::default()
            .key(pair.key().to_vec())
            .value(pair.get_value().await.unwrap())
            .cas_counter(pair.counter());
        items.push(entry);
    }

    debug!("It should be all failed because comparse_and_set failed!!!");
    for i in 0..n {
        let key = i.to_string().into_bytes();
        let value = (i + 100).to_string().into_bytes();
        let mut cc = items[i].get_cas_counter();
        let ret = kv.compare_and_set(key, value, cc + 1).await.unwrap_err();
        assert_eq!(ret.to_string(), Error::ValueCasMisMatch.to_string());
        assert_eq!(kv.to_ref().get_last_used_cas_counter() as usize, n + i + 1);
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    for (cas, item) in items.iter().enumerate() {
        assert_eq!(cas + 1, item.get_cas_counter() as usize);
    }

    // Although there are new key-value pairs successfully updated, the CAS (comparse_and_swap) value will still increment.
    assert_eq!(kv.to_ref().get_last_used_cas_counter(), 2 * n as u64);
    debug!(
        "change value to zzz{n} and the operation should be succeed because counter is right!!!"
    );
    for i in 0..n {
        let key = i.to_string().into_bytes();
        let value = format!("zzz{}", i).into_bytes();
        let ret = kv
            .compare_and_set(key, value, items[i].get_cas_counter())
            .await;
        assert!(ret.is_ok(), "{}", i);
    }
    debug!("cas has update, try it again");
    for i in 0..n {
        let key = i.to_string().into_bytes();
        let value = format!("zzz{}", i).as_bytes().to_vec();
        let got = kv.get_with_ext(&key).await.unwrap();
        let got = got.read().await;
        assert_eq!(got.get_value().await.unwrap(), value);
        assert_eq!(n * 2 + i + 1, got.counter() as usize);
    }
    info!("store path: {}", kv.opt.dir)
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
// WARNING: This test might take a while, but it should pass!
#[tokio::test]
async fn t_kv_get_more() {
    tracing_log();
    let kv = build_kv().await;
    let n = 3000;
    let m = 100;
    let mut entries = (0..n)
        .into_iter()
        .map(|i| i.to_string().as_bytes().to_vec())
        .map(|key| Entry::default().key(key.clone()).value(key.clone()))
        .collect::<Vec<_>>();
    for chunk in entries.chunks(m) {
        let ret = kv
            .batch_set(chunk.into_iter().map(|entry| entry.clone()).collect())
            .await;
        let pass = ret.into_iter().all(|ret| ret.is_ok());
        assert!(pass);
    }
    assert!(kv.must_lc().validate().is_ok());

    for entry in &entries {
        let got = kv.get(entry.key.as_ref()).await;
        assert!(got.is_ok());
        let value = got.unwrap();
        assert_eq!(value, entry.value);
    }

    // Overwrite
    entries.iter_mut().for_each(|entry| {
        entry.value = format!("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").into_bytes()
    });
    entries.reverse();
    for chunk in entries.chunks(m) {
        kv.batch_set(chunk.to_vec()).await;
    }

    for entry in &entries {
        let got = kv.get(entry.key.as_ref()).await;
        assert!(got.is_ok(), "{}", hex_str(entry.key.as_ref()));
        let value = got.unwrap();
        assert_eq!(
            hex_str(&value),
            hex_str(&entry.value),
            "#{}",
            hex_str(entry.key.as_ref())
        );
    }
}

async fn build_kv() -> XArc<KV> {
    use crate::test_util::{random_tmp_dir, tracing_log};
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    kv
}
