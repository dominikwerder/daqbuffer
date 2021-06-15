use crate::nodes::require_test_hosts_running;
use chrono::{DateTime, Utc};
use disk::binned::query::{BinnedQuery, CacheUsage};
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::{AggKind, Channel, Cluster, HostPort, NanoRange};
use std::time::Duration;

#[test]
fn get_binned_json_0() {
    taskrun::run(get_binned_json_0_inner()).unwrap();
}

async fn get_binned_json_0_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_binned_json_common(
        "scalar-i32-be",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T01:20:30.000Z",
        10,
        AggKind::DimXBins1,
        cluster,
        13,
        true,
    )
    .await
}

#[test]
fn get_binned_json_1() {
    taskrun::run(get_binned_json_1_inner()).unwrap();
}

async fn get_binned_json_1_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_binned_json_common(
        "wave-f64-be-n21",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T01:20:45.000Z",
        10,
        AggKind::DimXBins1,
        cluster,
        13,
        true,
    )
    .await
}

#[test]
fn get_binned_json_2() {
    taskrun::run(get_binned_json_2_inner()).unwrap();
}

async fn get_binned_json_2_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_binned_json_common(
        "wave-f64-be-n21",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T02:20:10.000Z",
        2,
        AggKind::DimXBinsN(0),
        cluster,
        2,
        true,
    )
    .await
}

async fn get_binned_json_common(
    channel_name: &str,
    beg_date: &str,
    end_date: &str,
    bin_count: u32,
    agg_kind: AggKind,
    cluster: &Cluster,
    expect_bin_count: u32,
    expect_finalised_range: bool,
) -> Result<(), Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let channel_backend = "testbackend";
    let channel = Channel {
        backend: channel_backend.into(),
        name: channel_name.into(),
    };
    let range = NanoRange::from_date_time(beg_date, end_date);
    let mut query = BinnedQuery::new(channel, range, bin_count, agg_kind);
    query.set_timeout(Duration::from_millis(15000));
    query.set_cache_usage(CacheUsage::Ignore);
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
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("get_binned_json_0  DONE  time {} ms", ms);
    let res = String::from_utf8(res.to_vec())?;
    let res: serde_json::Value = serde_json::from_str(res.as_str())?;
    info!(
        "result from endpoint: --------------\n{}\n--------------",
        serde_json::to_string_pretty(&res)?
    );
    if expect_finalised_range {
        if !res
            .get("finalisedRange")
            .ok_or(Error::with_msg("missing finalisedRange"))?
            .as_bool()
            .ok_or(Error::with_msg("key finalisedRange not bool"))?
        {
            return Err(Error::with_msg("expected finalisedRange"));
        }
    } else if res.get("finalisedRange").is_some() {
        return Err(Error::with_msg("expect absent finalisedRange"));
    }
    if res.get("counts").unwrap().as_array().unwrap().len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!("expect_bin_count {}", expect_bin_count)));
    }
    if res.get("mins").unwrap().as_array().unwrap().len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!("expect_bin_count {}", expect_bin_count)));
    }
    if res.get("maxs").unwrap().as_array().unwrap().len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!("expect_bin_count {}", expect_bin_count)));
    }
    if res.get("avgs").unwrap().as_array().unwrap().len() != expect_bin_count as usize {
        return Err(Error::with_msg(format!("expect_bin_count {}", expect_bin_count)));
    }
    Ok(())
}
