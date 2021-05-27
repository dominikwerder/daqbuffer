use crate::test::require_test_hosts_running;
use chrono::{DateTime, Utc};
use disk::cache::BinnedQuery;
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::{AggKind, Channel, Cluster, HostPort, NanoRange};

#[test]
fn get_binned_json_0() {
    taskrun::run(get_binned_json_0_inner()).unwrap();
}

async fn get_binned_json_0_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_binned_json_0_inner2(
        "wave-f64-be-n21",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T01:20:30.000Z",
        10,
        cluster,
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
    let agg_kind = AggKind::DimXBins1;
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let channel_backend = "testbackend";
    let channel = Channel {
        backend: channel_backend.into(),
        name: channel_name.into(),
    };
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = BinnedQuery::new(channel, range, bin_count, agg_kind);
    let url = query.url(&HostPort::from_node(node0));
    info!("get_binned_json_0  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url)
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
