use atomic::Atomic;
use chrono::Local;
use log::{info, kv::source::as_map, kv::Source, Level};
use rand::random;
use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::create_dir_all;
use std::io;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

#[cfg(test)]
pub(crate) fn mock_log() {
    use chrono::Local;
    use env_logger::Env;
    use log::kv::source::AsMap;
    use log::kv::{Error, Key, ToKey, ToValue, Value};
    use serde::{Deserialize, Serialize};
    use std::io::Write;

    #[derive(Serialize, Deserialize)]
    struct JsonLog {
        level: log::Level,
        ts: String,
        module: String,
        msg: String,
        #[serde(skip_serializing_if = "HashMap::is_empty", flatten)]
        kv: HashMap<String, serde_json::Value>,
    }

    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "error")
        .write_style_or("MY_LOG_STYLE", "always");
    let _ = env_logger::Builder::from_env(env)
        .format(|buf, record| {
            let mut l = JsonLog {
                ts: Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                module: record.file().unwrap_or("unknown").to_string()
                    + ":"
                    + &*record.line().unwrap_or(0).to_string(),
                level: record.level(),
                msg: record.args().to_string(),
                kv: Default::default(),
            };
            let kv: AsMap<&dyn Source> = as_map(record.key_values());
            if let Ok(kv) = serde_json::to_string(&kv) {
                let h: HashMap<String, serde_json::Value> = serde_json::from_str(&kv).unwrap();
                l.kv.extend(h.into_iter());
            }
            writeln!(buf, "{}", serde_json::to_string(&l).unwrap())
        })
        .try_init();
    log::info!( is_ok = true; "start init log");
    // env_logger::try_init_from_env(env);
}

#[cfg(test)]
pub(crate) fn mock_log_terminal() {
    console_log::init_with_level(Level::Debug);
}

#[cfg(test)]
pub(crate) fn tracing_log() {
    use tracing::{info, Level};
    use tracing_subscriber;
    struct LocalTimer;

    impl FormatTime for LocalTimer {
        fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
            write!(w, "{}", Local::now().format("%FT%T%.3f"))
        }
    }

    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        info!("panic info: {}", info);
        std::fs::write("out.put", info.to_string()).expect("TODO: panic message");
        std::process::exit(1);
    }));

    let _ = tracing_log::LogTracer::init();
    let format = tracing_subscriber::fmt::format()
        // .with_thread_names(true)
        .with_level(true)
        .with_target(true)
        .with_timer(LocalTimer);

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(io::stdout)
        .with_ansi(true)
        .event_format(format)
        .try_init();
    tracing::info!("log setting done");
}

pub fn random_tmp_dir() -> String {
    let id = random::<u32>();
    let path = temp_dir().join(id.to_string()).join("badger");
    path.to_str().unwrap().to_string()
}

#[test]
fn it_work() {
    #[tracing::instrument(skip_all)]
    fn call() {
        info!("call c");
    }

    #[tracing::instrument(skip_all)]
    fn my_function(my_arg: usize) {
        info!("execute my function");
        call();
    }

    tracing_log();
    my_function(1000);
    info!("Hello Body");
}

#[tokio::test]
async fn runtime_tk() {
    use tokio::{sync::RwLock, task::JoinHandle};

    pub type Future<T> = JoinHandle<T>;
    pub type SafeFn<A> = Arc<RwLock<dyn FnMut(A) -> Option<A> + Sync + Send>>;
    pub struct SafeFnWrapper<A> {
        fn_mut: SafeFn<A>,
    }

    impl<A: Sync + Send + 'static> SafeFnWrapper<A> {
        pub fn new(fn_mut: impl FnMut(A) -> Option<A> + Send + Sync + 'static) -> SafeFnWrapper<A> {
            SafeFnWrapper::set(Arc::new(RwLock::new(fn_mut)))
        }

        pub fn set(fn_mut: SafeFn<A>) -> Self {
            Self { fn_mut }
        }

        /// Get a clone of the `fn_mut` field (which holds a thread safe `FnMut`).
        pub fn get(&self) -> SafeFn<A> {
            self.fn_mut.clone()
        }

        /// This is an `async` function. Make sure to use `await` on the return value.
        pub fn spawn(&self, action: A) -> Future<Option<A>> {
            let arc_lock_fn_mut = self.get();
            tokio::spawn(async move {
                // Delay before calling the function.
                // let delay_ms = rand::thread_rng().gen_range(100..1_000) as u64;
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                let mut fn_mut = arc_lock_fn_mut.write().await; // 👀 `unwrap()` for blocking.
                fn_mut(action)
            })
        }
    }
    //
    fn load() -> impl FnMut(i32) -> Option<i32> {
        |_| {
            println!("HeLoo");
            Some(299)
        }
    }
}

#[test]
fn tk2() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let a = Arc::new(std::sync::atomic::AtomicI32::new(10000000));
    let ac = a.clone();
    rt.block_on(async move {
        for i in 0..10000 {
            let ac = ac.clone();
            tokio::spawn(async move {
                ac.fetch_sub(1, Ordering::Relaxed);
            });
        }
        fn add() -> i32 {
            let f = async move { 100 };
            let r =
                tokio::task::block_in_place(move || tokio::runtime::Handle::current().block_on(f));
            r
        }

        let ret = add();
        println!("return {}", ret);
    });
    println!("{}", a.load(Ordering::Relaxed));
}
