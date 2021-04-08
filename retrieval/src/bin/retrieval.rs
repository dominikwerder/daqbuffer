#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;

pub fn main() {
    match taskrun::run(go()) {
        Ok(k) => {
            info!("{:?}", k);
        }
        Err(k) => {
            error!("{:?}", k);
        }
    }
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

#[test]
fn simple_fetch() {
    taskrun::run(async {
        let t1 = chrono::Utc::now();
        let query = netpod::AggQuerySingleChannel {
            ksprefix: "daq_swissfel".into(),
            keyspace: 3,
            channel: netpod::Channel {
                name: "S10BC01-DBAM070:BAM_CH1_NORM".into(),
                backend: "sf-databuffer".into(),
            },
            timebin: 18720,
            tb_file_count: 1,
            split: 12,
            tbsize: 1000 * 60 * 60 * 24,
            buffer_size: 1024 * 8,
        };
        let query_string = serde_json::to_string(&query).unwrap();
        let _host = tokio::spawn(httpret::host(8360));
        let req = hyper::Request::builder()
            .method(http::Method::POST)
            .uri("http://localhost:8360/api/1/parsed_raw")
            .body(query_string.into()).unwrap();
        let client = hyper::Client::new();
        let res = client.request(req).await?;
        info!("client response {:?}", res);
        let mut res_body = res.into_body();
        use hyper::body::HttpBody;
        let mut ntot = 0 as u64;
        loop {
            match res_body.data().await {
                Some(Ok(k)) => {
                    //info!("packet..  len {}", k.len());
                    ntot += k.len() as u64;
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
        let t2 = chrono::Utc::now();
        let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
        let throughput = ntot / 1024 * 1000 / ms;
        info!("total download {} MB   throughput {:5} kB/s", ntot / 1024 / 1024, throughput);
        //Err::<(), _>(format!("test error").into())
        Ok(())
    }).unwrap();
}
