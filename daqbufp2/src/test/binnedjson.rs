use crate::nodes::{require_sls_test_host_running, require_test_hosts_running};
use chrono::{DateTime, Utc};
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::query::{BinnedQuery, CacheUsage};
use netpod::{log::*, AppendToUrl};
use netpod::{AggKind, Channel, Cluster, NanoRange, APP_JSON};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use url::Url;

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
        "1970-01-01T00:20:20.000Z",
        2,
        AggKind::DimXBinsN(3),
        cluster,
        2,
        true,
    )
    .await
}

#[test]
fn get_sls_archive_1() -> Result<(), Error> {
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ABOMA-CH-6G:U-DCLINK".into(),
        };
        let begstr = "2021-11-10T01:00:00Z";
        let endstr = "2021-11-10T01:01:00Z";
        let res = get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        assert_eq!(res.finalised_range, true);
        assert_eq!(res.ts_anchor, 1636506000);
        //assert!((res.avgs[3].unwrap() - 1.01470947265625).abs() < 1e-4);
        //assert!((res.avgs[4].unwrap() - 24.06792449951172).abs() < 1e-4);
        //assert!((res.avgs[11].unwrap() - 0.00274658203125).abs() < 1e-4);
        Ok(())
    };
    taskrun::run(fut)
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
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", node0.host, node0.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("get_binned_json_common  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_JSON)
        .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    if res.status() != StatusCode::OK {
        error!("get_binned_json_common client response {:?}", res);
    }
    let res = hyper::body::to_bytes(res.into_body()).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    debug!("get_binned_json_common  DONE  time {} ms", ms);
    let res = String::from_utf8_lossy(&res).to_string();
    let res: serde_json::Value = serde_json::from_str(res.as_str())?;
    // TODO assert more
    debug!(
        "result from endpoint: --------------\n{}\n--------------",
        serde_json::to_string_pretty(&res)?
    );
    // TODO enable in future:
    if false {
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

#[derive(Debug, Serialize, Deserialize)]
struct BinnedResponse {
    #[serde(rename = "tsAnchor")]
    ts_anchor: u64,
    #[serde(rename = "tsMs")]
    ts_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_ns: Vec<u64>,
    mins: Vec<Option<f64>>,
    maxs: Vec<Option<f64>>,
    avgs: Vec<Option<f64>>,
    counts: Vec<u64>,
    #[serde(rename = "finalisedRange", default = "bool_false")]
    finalised_range: bool,
}

fn bool_false() -> bool {
    false
}

async fn get_binned_json_common_res(
    channel: Channel,
    beg_date: &str,
    end_date: &str,
    bin_count: u32,
    agg_kind: AggKind,
    cluster: &Cluster,
) -> Result<BinnedResponse, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let mut query = BinnedQuery::new(channel, range, bin_count, agg_kind);
    query.set_timeout(Duration::from_millis(15000));
    query.set_cache_usage(CacheUsage::Ignore);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", node0.host, node0.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("get_binned_json_common  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_JSON)
        .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    if res.status() != StatusCode::OK {
        error!("get_binned_json_common client response {:?}", res);
    }
    let res = hyper::body::to_bytes(res.into_body()).await?;
    let t2 = chrono::Utc::now();
    let _ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    let res = String::from_utf8_lossy(&res).to_string();
    info!("GOT: {}", res);
    let res: BinnedResponse = serde_json::from_str(res.as_str())?;
    Ok(res)
}
