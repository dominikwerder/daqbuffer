use crate::spawn_test_hosts;
use crate::test::test_cluster;
use chrono::{DateTime, Utc};
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::{ByteSize, Cluster};

#[test]
fn get_binned_json_0() {
    taskrun::run(get_binned_json_0_inner()).unwrap();
}

async fn get_binned_json_0_inner() -> Result<(), Error> {
    let cluster = test_cluster();
    let _hosts = spawn_test_hosts(cluster.clone());
    get_binned_json_0_inner2(
        "wave-f64-be-n21",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T01:20:30.000Z",
        10,
        &cluster,
    )
    .await
}

async fn get_binned_json_0_inner2(
    channel_name: &str,
    beg_date: &str,
    end_date: &str,
    bin_count: u32,
    cluster: &Cluster,
) -> Result<(), Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let channel_backend = "testbackend";
    let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
    let disk_stats_every = ByteSize::kb(1024);
    // TODO have a function to form the uri, including perf opts:
    let uri = format!(
        "http://{}:{}/api/4/binned?cache_usage=ignore&channel_backend={}&channel_name={}&bin_count={}&beg_date={}&end_date={}&disk_stats_every_kb={}",
        node0.host,
        node0.port,
        channel_backend,
        channel_name,
        bin_count,
        beg_date.format(date_fmt),
        end_date.format(date_fmt),
        disk_stats_every.bytes() / 1024,
    );
    info!("get_binned_json_0  get {}", uri);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .header("Accept", "application/json")
        .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
    }
    let res = hyper::body::to_bytes(res.into_body()).await?;
    let res = String::from_utf8(res.to_vec())?;
    info!("result from endpoint: [[[\n{}\n]]]", res);
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("get_binned_json_0  DONE  time {} ms", ms);
    Ok(())
}
