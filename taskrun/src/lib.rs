#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::panic;
use tokio::task::JoinHandle;
use std::future::Future;

pub fn run<T, F: std::future::Future<Output=Result<T, Error>>>(f: F) -> Result<T, Error> {
    tracing_init();
    tokio::runtime::Builder::new_multi_thread()
    .worker_threads(12)
    .max_blocking_threads(256)
    .enable_all()
    .on_thread_start(|| {
        let old = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            error!("✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗     panicking\n{:?}\nLOCATION: {:?}\nPAYLOAD: {:?}", backtrace::Backtrace::new(), info.location(), info.payload());
            //old(info);
        }));
    })
    .build()
    .unwrap()
    .block_on(async {
        f.await
    })
}

pub fn tracing_init() {
    tracing_subscriber::fmt()
    //.with_timer(tracing_subscriber::fmt::time::uptime())
    .with_target(true)
    .with_thread_names(true)
    //.with_max_level(tracing::Level::INFO)
    .with_env_filter(tracing_subscriber::EnvFilter::new("info,retrieval=trace,disk=trace,tokio_postgres=info"))
    .init();
}


pub fn spawn<T>(task: T) -> JoinHandle<T::Output> where T: Future + Send + 'static, T::Output: Send + 'static {
    tokio::spawn(task)
}
