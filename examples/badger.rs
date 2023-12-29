use std::path::PathBuf;
use structopt::StructOpt;


#[derive(StructOpt)]
#[structopt(about = "BadgerDB demo")]
enum Opt {
    BackUp,
}


#[tokio::main]
async fn main() {

    let env = tracing_subscriber::EnvFilter::from_default_env();
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(env)
        .try_init()
        .unwrap();


    let opt = Opt::from_args();
    match opt {
        Opt::BackUp => {
            let opt = badger_rs::Options::default();
            let kv = badger_rs::DB::open(opt).await.unwrap();
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
    }
}