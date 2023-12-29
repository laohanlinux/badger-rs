use atomic::Atomic;
use chrono::Local;
use log::{info, kv::source::as_map, kv::Source, warn, Level};
use rand::random;
use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::create_dir_all;
use std::io;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio_metrics::TaskMonitor;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::EnvFilter;
use crate::Options;

#[cfg(test)]
pub fn push_log_by_filename(fpath: &str, buf: &[u8]) {
    use std::io::Write;
    let mut fp = std::fs::File::options()
        .write(true)
        .append(true)
        .create(true)
        .open(fpath)
        .unwrap();
    fp.write_all(buf).unwrap();
    fp.write_all(b"\n").unwrap();
}

#[cfg(test)]
pub fn push_log(buf: &[u8], rd: bool) {
    // #[cfg(test)]
    // return;
    use std::io::Write;
    let mut fpath = "raw_log.log";
    let mut fp = if !rd {
        std::fs::File::options()
            .write(true)
            .append(true)
            .create(true)
            .open(fpath)
            .unwrap()
    } else {
        std::fs::File::options()
            .write(true)
            .append(true)
            .create(true)
            .open(random_tmp_dir() + "/" + fpath)
            .unwrap()
    };
    fp.write_all(buf).unwrap();
    fp.write_all(b"\n").unwrap();
}

#[cfg(test)]
pub fn remove_push_log() {
    use std::fs::remove_file;
    remove_file("raw_log.log");
}

#[cfg(test)]
pub(crate) fn tracing_log() {
    use libc::remove;
    use tracing::{info, Level};
    use tracing_subscriber;
    struct LocalTimer;

    impl FormatTime for LocalTimer {
        fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
            write!(w, "{}", Local::now().format("%T"))
        }
    }

    // let default_panic = std::panic::take_hook();
    // std::panic::set_hook(Box::new(move |info| {
    //     default_panic(info);
    //     info!("panic info: {}", info);
    //     std::fs::write("out.put", info.to_string()).expect("TODO: panic message");
    //     std::process::exit(1);
    // }));

    unsafe { backtrace_on_stack_overflow::enable() };

    let format = tracing_subscriber::fmt::format()
        .with_thread_ids(true)
        .with_level(true)
        .with_target(true)
        .with_line_number(true)
        .with_timer(LocalTimer);

    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(io::stdout)
        .with_ansi(true)
        .event_format(format)
        .try_init();
    remove_push_log();
    // let recorder = metrics_prometheus::install();
}

#[cfg(test)]
pub(crate) async fn start_metrics() -> TaskMonitor {
    let monitor = tokio_metrics::TaskMonitor::new();
    // print task metrics every 500ms
    {
        let frequency = std::time::Duration::from_millis(500);
        let monitor = monitor.clone();
        tokio::spawn(async move {
            for metrics in monitor.intervals() {
                warn!("{:?}", metrics);
                tokio::time::sleep(frequency).await;
            }
        });
    }
    monitor
}

pub fn random_tmp_dir() -> String {
    let id = random::<u32>();
    let path = temp_dir().join(id.to_string()).join("badger");
    path.to_str().unwrap().to_string()
}

pub fn create_random_tmp_dir() -> String {
    let fpath = random_tmp_dir();
    create_dir_all(&fpath).unwrap();
    fpath
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

    use itertools::Merge;
}


pub(crate) fn get_test_option(dir: &str) -> Options {
    let mut opt = Options::default();
    opt.max_table_size = 1 << 15; // Force more compaction.
    opt.level_one_size = 4 << 15; // Force more compaction.
    opt.dir = Box::new(dir.to_string());
    opt.value_dir = Box::new(dir.to_string());
    opt
}
