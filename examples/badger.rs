#[tokio::main]
async fn main() {
    let opt = badger_rs::Options::default();
    let kv = badger_rs::KV::open(opt).await.unwrap();
    kv.set(
        b"hello word".to_vec(),
        b">>>>>I LOVE YOU!<<<<<".to_vec(),
        0x0,
    )
    .await
    .unwrap();

    let got = kv.get(b"hello word").await.unwrap();
    println!("{}", String::from_utf8_lossy(&got));
}
