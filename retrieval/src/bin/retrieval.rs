#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use http::Method;
use hyper::Body;

pub fn main() {
    match run(go()) {
        Ok(k) => {
            info!("{:?}", k);
        }
        Err(k) => {
            error!("{:?}", k);
        }
    }
}

fn run<T, F: std::future::Future<Output=Result<T, Error>>>(f: F) -> Result<T, Error> {
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

async fn go() -> Result<(), Error> {
    use clap::Clap;
    use retrieval::cli::{Opts, SubCmd};
    let opts = Opts::parse();
    match opts.subcmd {
        SubCmd::Retrieval(_subcmd) => {
            trace!("testout");
            info!("testout");
            error!("testout");
        }
    }
    Ok(())
}

pub fn tracing_init() {
    tracing_subscriber::fmt()
        //.with_timer(tracing_subscriber::fmt::time::uptime())
        .with_target(true)
        .with_thread_names(true)
        //.with_max_level(tracing::Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::new("info,retrieval=trace,tokio_postgres=info"))
        .init();
}

#[test]
fn simple_fetch() {
    run(async {
        let query = netpod::AggQuerySingleChannel {
            channel: "S10CB01-RIQM-STA:DACI".into(),
        };
        let query_string = serde_json::to_string(&query).unwrap();
        let host = tokio::spawn(httpret::host(8360));
        let req = hyper::Request::builder()
            .method(Method::POST)
            .uri("http://localhost:8360/api/1/parsed_raw")
            .body(query_string.into()).unwrap();
        let client = hyper::Client::new();
        let res = client.request(req).await?;
        info!("client response {:?}", res);
        let mut res_body = res.into_body();
        use hyper::body::HttpBody;
        loop {
            match res_body.data().await {
                Some(Ok(k)) => {
                    info!("packet..  len {}", k.len());
                }
                Some(Err(e)) => {
                    error!("{:?}", e);
                }
                None => {
                    info!("response stream exhausted");
                    break;
                }
            }
        }
        //Err::<(), _>(format!("test error").into())
        Ok(())
    }).unwrap();
}
