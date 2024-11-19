use crate::test_util::get_test_option;
use crate::test_util::{random_tmp_dir, tracing_log};
use crate::DB;

#[tokio::test]
async fn t_txn_simple() {
    tracing_log();
    let dir = random_tmp_dir();
    let kv = DB::open(get_test_option(&dir)).await;
    let kv = kv.unwrap();

    let txn = kv.new_transaction(true).unwrap();

    for i in 0..10 {
        let k = format!("key={}", i);
        let v = format!("value={}", i);
        txn.set(k.as_bytes().to_vec(), v.as_bytes().to_vec(), 0);
    }

    let item = txn.get(b"key=8").await;
    assert!(item.is_ok());
    let value = item.unwrap().value().await.unwrap();
    assert_eq!(&value, b"value=8");

    let commit = txn.commit().await;
    assert!(commit.is_ok());
}

#[tokio::test]
async fn t_txn_version() {
    tracing_log();
    let dir = random_tmp_dir();
    let kv = DB::open(get_test_option(&dir)).await.unwrap();
    let key = b"key";
    for i in 1..=10 {
        let txn = kv.new_transaction(true).unwrap();
        txn.set(key.to_vec(), format!("valueversion={}", i).into_bytes(), 0);
        let cmt = txn.commit().await;
        assert!(cmt.is_ok());
        let read_ts = kv.txn_state.read_ts().await;
        assert_eq!(read_ts, i);
    }
}
