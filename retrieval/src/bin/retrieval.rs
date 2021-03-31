use err::Error;

pub fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .max_blocking_threads(256)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::new("daqbuffer=trace,tokio_postgres=info"))
                .init();
            go().await;
        })
}

async fn go() {
    match inner().await {
        Ok(_) => (),
        Err(_) => ()
    }
}

async fn inner() -> Result<(), Error> {
    use clap::Clap;
    use retrieval::cli::Opts;
    let _opts = Opts::parse();
    Ok(())
}

pub fn tracing_init() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("retrieval=trace,tokio_postgres=info"))
        .init();
}

#[test]
fn t1() {
    tracing_init();
    tracing::error!("parsed");
    //panic!();
}
