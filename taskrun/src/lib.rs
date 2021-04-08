use err::Error;

pub fn run<T, F: std::future::Future<Output=Result<T, Error>>>(f: F) -> Result<T, Error> {
    tracing_init();
    tokio::runtime::Builder::new_multi_thread()
    .worker_threads(12)
    .max_blocking_threads(256)
    .enable_all()
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
