use awaitgroup::WaitGroup;
use drop_cell::defer;
use log::{debug, info, warn};
use std::collections::HashSet;
use std::env::temp_dir;
use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;
use tokio::pin;
use tokio_stream::StreamExt;
use tracing_subscriber::fmt::format;

use crate::iterator::IteratorOptions;
use crate::test_util::{push_log, remove_push_log, tracing_log};
use crate::types::{TArcMx, XArc};
use crate::value_log::{Entry, MetaBit, MAX_KEY_SIZE};
use crate::y::hex_str;
use crate::{kv::KV, options::Options, Error};

fn get_test_option(dir: &str) -> Options {
    let mut opt = Options::default();
    opt.max_table_size = 1 << 15; // Force more compaction.
    opt.level_one_size = 4 << 15; // Force more compaction.
    opt.dir = Box::new(dir.to_string());
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
        count_set.insert(hex_str(kv_item.key()));
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
        let cc = items[i].get_cas_counter();
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
    tracing_log();
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
    let n = 10000;
    let m = 100;
    // first version
    let mut entries = (0..n)
        .into_iter()
        .map(|i| i.to_string().as_bytes().to_vec())
        .map(|key| {
            Entry::default()
                .key(key.clone())
                .value(key.clone())
                .user_meta(1)
        })
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

    // Overwrite with version 2
    entries.iter_mut().for_each(|entry| {
        entry.user_meta = 2;
        entry.value = format!("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").into_bytes();
    });
    entries.reverse();
    for chunk in entries.chunks(m) {
        kv.batch_set(chunk.to_vec()).await;
    }
    assert!(kv.must_lc().validate().is_ok());

    for entry in &entries {
        let got = kv.get(entry.key.as_ref()).await;
        assert!(
            got.is_ok(),
            "{}, err:{}",
            hex_str(entry.key.as_ref()),
            got.unwrap_err()
        );
        let value = got.unwrap();
        assert_eq!(
            hex_str(&value),
            hex_str(&entry.value),
            "#{}",
            hex_str(entry.key.as_ref())
        );
    }

    // "Delete" key.
    entries.iter_mut().for_each(|entry| {
        entry.user_meta = 3;
        entry.meta = MetaBit::BIT_DELETE.bits();
        entry.value = b"Hiz".to_vec();
    });

    for chunk in entries.chunks(m) {
        kv.batch_set(chunk.to_vec()).await;
    }
    assert!(kv.must_lc().validate().is_ok());

    for entry in &entries {
        let got = kv.get(entry.key.as_ref()).await;
        assert!(got.unwrap_err().is_not_found());
    }
}

// Put a lot of data to move some data to disk.
// WARNING: This test might take a while but it should pass!
#[tokio::test]
async fn t_kv_exists_more() {
    tracing_log();
    let start = SystemTime::now();
    let kv = build_kv().await;

    let n = 10000;
    let m = 100;
    // first version
    let mut entries = (0..n)
        .into_iter()
        .map(|i| i.to_string().as_bytes().to_vec())
        .map(|key| {
            Entry::default()
                .key(key.clone())
                .value(key.clone())
                .user_meta(1)
        })
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
        let got = kv.exists(entry.key.as_ref()).await;
        assert!(got.is_ok());
        assert!(got.unwrap());
    }

    let got = kv.exists(b"not-exists").await;
    assert!(got.is_ok());
    assert!(!got.unwrap());

    info!(
        "after check exists, cost {}ms",
        SystemTime::now().duration_since(start).unwrap().as_millis()
    );

    // "Delete" key.
    entries.iter_mut().for_each(|entry| {
        entry.user_meta = 3;
        entry.meta = MetaBit::BIT_DELETE.bits();
    });

    for chunk in entries.chunks(m) {
        kv.batch_set(chunk.to_vec()).await;
    }
    assert!(kv.must_lc().validate().is_ok());
    info!(
        "after deleted, cost {}ms",
        SystemTime::now().duration_since(start).unwrap().as_millis()
    );

    for entry in &entries {
        let got = kv.exists(entry.key.as_ref()).await;
        assert!(got.is_ok());
        assert!(!got.unwrap());
    }

    info!(
        "Done and closing, cost {}ms",
        SystemTime::now().duration_since(start).unwrap().as_millis()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_iterator_basic() {
    tracing_log();
    let kv = build_kv().await;

    let n = 10000;

    let bkey = |i: usize| format!("{:09}", i).as_bytes().to_vec();
    let bvalue = |i: usize| format!("{:025}", i).as_bytes().to_vec();
    let entries = (0..n)
        .into_iter()
        .map(|i| (bkey(i), bvalue(i)))
        .map(|(key, value)| Entry::default().key(key).value(value).user_meta(1))
        .collect::<Vec<_>>();
    for entry in entries.iter() {
        kv.set(entry.key.clone(), entry.value.clone(), 0)
            .await
            .unwrap();
    }
    assert!(kv.must_lc().validate().is_ok());

    let mut opt = IteratorOptions::default();
    opt.pre_fetch_values = true;
    opt.pre_fetch_size = 10;

    {
        let itr = kv.new_iterator(opt).await;
        let mut count = 0;
        let mut rewind = true;
        info!("Startinh first basic iteration");
        itr.rewind().await;
        while let Some(item) = itr.peek().await {
            let rd_item = item.read().await;
            let key = rd_item.key();
            if rewind && count == 5000 {
                // Rewind would be skip /heap/key, and it.next() would be skip 0.
                count = 0;
                let _ = itr.rewind().await.unwrap();
                rewind = false;
                continue;
            }
            assert_eq!(
                hex_str(key),
                hex_str(&bkey(count)),
                "count = {}, rewind = {}",
                count,
                rewind
            );
            let val = rd_item.get_value().await.unwrap();
            assert_eq!(hex_str(&val), hex_str(&bvalue(count)));
            count += 1;
            itr.next().await;
        }
        assert_eq!(count, entries.len());
    }

    {
        info!("Starting second basic iteration");
        let mut idx = 5030;
        let start = bkey(idx);
        let itr = kv.new_iterator(opt).await;
        let _ = itr.seek(&start).await.unwrap();
        while let Some(item) = itr.peek().await {
            let item = item.read().await;
            assert_eq!(hex_str(&bkey(idx)), hex_str(item.key()));
            assert_eq!(
                hex_str(&bvalue(idx)),
                hex_str(&item.get_value().await.unwrap())
            );
            idx += 1;
            itr.next().await;
        }
    }
}

#[tokio::test]
async fn t_kv_load() {
    tracing_log();
    let kv = build_kv().await;

    let n = 10000;

    let bkey = |i: usize| format!("{:09}", i).as_bytes().to_vec();
    let bvalue = |i: usize| format!("{:025}", i).as_bytes().to_vec();
    let entries = (0..n)
        .into_iter()
        .map(|i| (bkey(i), bvalue(i)))
        .map(|(key, value)| Entry::default().key(key).value(value).user_meta(1))
        .collect::<Vec<_>>();
    for entry in entries.iter() {
        kv.set(entry.key.clone(), entry.value.clone(), 0)
            .await
            .unwrap();
    }

    assert!(kv.must_lc().validate().is_ok());

    kv.close().await.unwrap();
    let mut fids = kv.must_lc().get_all_fids();
    let dir = kv.opt.dir.clone();

    // Check that files are garbage collected.
    let mut disk_fids = crate::table::table::get_id_map(dir.as_str())
        .into_iter()
        .collect::<Vec<_>>();
    fids.sort();
    disk_fids.sort();
    assert_eq!(fids, disk_fids);
}

#[tokio::test]
async fn t_kv_iterator_deleted() {
    tracing_log();
    let kv = build_kv().await;
    kv.set(b"Key1".to_vec(), b"Value1".to_vec(), 0x00)
        .await
        .unwrap();
    kv.set(b"Key2".to_vec(), b"Value2".to_vec(), 0x00)
        .await
        .unwrap();

    let mut opt = IteratorOptions::default();
    opt.pre_fetch_size = 10;
    let itr = kv.new_iterator(opt).await;
    let mut wb = vec![];
    let prefix = b"Key";
    itr.seek(prefix).await;
    while let Some(item) = itr.peek().await {
        let key = item.read().await.key().to_vec();
        if !key.starts_with(prefix) {
            break;
        }
        let entry = Entry::default().key(key).meta(MetaBit::BIT_DELETE.bits());
        wb.push(entry);
        itr.next().await;
    }
    assert_eq!(2, wb.len());
    let ret = kv.batch_set(wb).await;
    for res in &ret {
        assert!(res.is_ok());
    }

    for prefetch in [true, false] {
        let mut opt = IteratorOptions::default();
        opt.pre_fetch_values = prefetch;
        let idx_it = kv.new_iterator(opt).await;
        let mut est_size = 0;
        let mut idx_keys = vec![];
        idx_it.seek(prefix).await;
        while let Some(item) = idx_it.peek().await {
            let item = item.read().await;
            let key = item.key().to_vec();
            est_size += item.estimated_size();
            if !key.starts_with(prefix) {
                break;
            }

            idx_keys.push(hex_str(&key));
            idx_it.next().await;
        }

        assert_eq!(0, idx_keys.len());
        assert_eq!(0, est_size);
    }
}

#[tokio::test]
async fn t_delete_without_sync_write() {
    tracing_log();
    let kv = build_kv().await;
    let key = b"k1".to_vec();
    kv.set(
        key.clone(),
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890".to_vec(),
        0x00,
    )
        .await
        .unwrap();
    let opt = kv.opt.clone();
    kv.delete(&key).await.unwrap();
    kv.close().await.expect("TODO: panic message");
    drop(kv);
    // Reopen kv, it should failed
    {
        let kv = KV::open(opt).await.unwrap();
        let got = kv.get_with_ext(&key).await;
        assert!(got.unwrap_err().is_not_found());
    }
}

#[tokio::test]
async fn t_kv_set_if_absent() {
    tracing_log();
    let kv = build_kv().await;
    let key = b"k1".to_vec();
    let got = kv
        .set_if_ab_sent(key.clone(), b"value".to_vec(), 0x00)
        .await;
    assert!(got.is_ok());

    let got = kv
        .set_if_ab_sent(key.clone(), b"value2".to_vec(), 0x00)
        .await;
    assert!(got.unwrap_err().is_exists());
}

#[tokio::test]
async fn t_kv_pid_file() {
    tracing_log();
    let kv1 = build_kv().await;
    let kv2 = KV::open(kv1.opt.clone()).await;
    let err = kv2.unwrap_err();
    assert!(err
        .to_string()
        .contains("Another program process is using the Badger databse"));
    kv1.close().await.unwrap();
    drop(kv1);
}

#[tokio::test]
async fn t_kv_big_key_value_pairs() {
    tracing_log();
    let kv = build_kv().await;
    let big_key = vec![0u8; MAX_KEY_SIZE + 1];
    let big_value = vec![0u8; kv.opt.value_log_file_size as usize + 1];
    let small = vec![0u8; 10];
    let got = kv.set(big_key.clone(), small.clone(), 0x00).await;
    let err = got.unwrap_err();
    assert!(err.to_string().starts_with("Key"));

    let got = kv.set(small.clone(), big_value.clone(), 0x00).await;
    let err = got.unwrap_err();
    assert!(err.to_string().starts_with("Value"));

    let entry1 = Entry::default().key(small.clone()).value(small.clone());
    let entry2 = Entry::default()
        .key(big_key.clone())
        .value(big_value.clone());
    let res = kv.batch_set(vec![entry1, entry2]).await;
    let first_res = res[0].clone();
    let second_res = res[1].clone();
    assert!(first_res.is_ok());
    assert!(second_res.unwrap_err().to_string().starts_with("Key"));

    // make sure e1 was actually set:
    let item = kv.get(&small).await.unwrap();
    assert_eq!(&item, &small);
    kv.close().await.unwrap();
}

#[tokio::test]
async fn t_kv_iterator_prefetch_size() {
    tracing_log();
    let kv = build_kv().await;
    let bkey = |i: usize| format!("{:09}", i).as_bytes().to_vec();
    let bvalue = |i: usize| format!("{:025}", i).as_bytes().to_vec();

    let n = 100;
    let entries = (0..n)
        .into_iter()
        .map(|i| (bkey(i), bvalue(i)))
        .map(|(key, value)| Entry::default().key(key).value(value).user_meta(1))
        .collect::<Vec<_>>();
    for entry in entries.iter() {
        kv.set(entry.key.clone(), entry.value.clone(), 0)
            .await
            .unwrap();
    }

    for prefetch_size in [-10, 0, 1, 10] {
        let mut opt = IteratorOptions::default();
        opt.pre_fetch_values = true;
        opt.pre_fetch_size = prefetch_size;
        let itr = kv.new_iterator(opt).await;
        itr.rewind().await;
        let mut count = 0;
        while itr.peek().await.is_some() {
            count += 1;
            itr.next().await;
        }
        assert_eq!(count, n);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t_kv_get_set_race() {
    use rand::{thread_rng, Rng};
    tracing_log();
    let mut rng = thread_rng();
    let mut arr = [0u8; 4096];
    rng.fill(&mut arr);
    let kv = build_kv().await;
    let mut wg = awaitgroup::WaitGroup::new();
    let num_op = 1000;
    let data = TArcMx::new(tokio::sync::Mutex::new(arr));

    let worker = wg.worker();
    let ndata = data.clone();
    let mut key_ch = crate::types::Channel::new(1);
    let key_tx = key_ch.tx();
    {
        let kv = kv.clone();
        tokio::spawn(async move {
            for i in 0..num_op {
                let key = format!("{}", i);
                let data = ndata.lock().await.clone().to_vec();
                kv.set(key.clone().into_bytes(), data, 0x0).await.unwrap();
                key_tx.send(key.into_bytes()).await.unwrap();
                info!("Put Some");
            }
            worker.done();
            key_tx.close();
        });
    }

    let worker = wg.worker();
    let key_rx = key_ch.rx();

    {
        let kv = kv.clone();
        tokio::spawn(async move {
            while let Ok(key) = key_rx.recv().await {
                kv.get(&key).await.unwrap();
                info!("Try again!");
            }
            worker.done();
        });
    }
    wg.wait().await;
    kv.close().await.expect("TODO: panic message");
}

extern crate test;

// #[bench]
// fn t_kv_bench_exists(b: &mut test::Bencher) {
//     let mut r = tokio::runtime::Runtime::new().unwrap();
//     r.block_on(
//
//         async {
//             let kv = build_kv().await;
//             let n = 50000 * 100;
//             let m = 100;
//             let mut entries = (0..n)
//                 .into_iter()
//                 .map(|i| i.to_string().as_bytes().to_vec())
//                 .map(|key| {
//                     Entry::default()
//                         .key(key.clone())
//                         .value(key.clone())
//                         .user_meta(1)
//                 })
//                 .collect::<Vec<_>>();
//             for chunk in entries.chunks(m) {
//                 let ret = kv
//                     .batch_set(chunk.into_iter().map(|entry| entry.clone()).collect())
//                     .await;
//                 let pass = ret.into_iter().all(|ret| ret.is_ok());
//                 assert!(pass);
//             }
//             assert!(kv.must_lc().validate().is_ok());
//             // Get
//             b.iter(|| {
//                 for i in 0..n {
//
//                 }
//             });
//         }
//     );
// }


async fn build_kv() -> XArc<KV> {
    use crate::test_util::random_tmp_dir;
    tracing_log();
    let dir = random_tmp_dir();
    let kv = KV::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();
    kv
}
