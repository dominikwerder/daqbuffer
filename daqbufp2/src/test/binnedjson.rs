use crate::nodes::{require_sls_test_host_running, require_test_hosts_running};
use chrono::{DateTime, Utc};
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::query::{BinnedQuery, CacheUsage};
use netpod::{f64_close, AppendToUrl};
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
        let exp = r##"{"avgs":[24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875],"counts":[0,0,0,0,0,0,0,0,0,0,0,0],"finalisedRange":true,"maxs":[24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875],"mins":[24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875,24.37225341796875],"tsAnchor":1636506000,"tsMs":[0,5000,10000,15000,20000,25000,30000,35000,40000,45000,50000,55000,60000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        res.is_close(&exp)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_sls_archive_2() -> Result<(), Error> {
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-PCT:CURRENT".into(),
        };
        let begstr = "2021-11-10T00:00:00Z";
        let endstr = "2021-11-10T00:10:00Z";
        let res = get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[401.1745910644531,401.5135498046875,400.8823547363281,400.66156005859375,401.8301086425781,401.19305419921875,400.5584411621094,401.4371337890625,401.4137268066406,400.77880859375],"counts":[19,6,6,19,6,6,6,19,6,6],"finalisedRange":true,"maxs":[402.04977411361034,401.8439029736943,401.22628955394583,402.1298351124666,402.1298351124666,401.5084092642013,400.8869834159359,402.05358654212733,401.74477983225313,401.1271664125047],"mins":[400.08256099885625,401.22628955394583,400.60867613419754,400.0939982844072,401.5084092642013,400.8869834159359,400.2693699961876,400.05968642775446,401.1271664125047,400.50574056423943],"tsAnchor":1636502400,"tsMs":[0,60000,120000,180000,240000,300000,360000,420000,480000,540000,600000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        res.is_close(&exp)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_sls_archive_3() -> Result<(), Error> {
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-PCT:CURRENT".into(),
        };
        let begstr = "2021-11-09T00:00:00Z";
        let endstr = "2021-11-11T00:10:00Z";
        let res = get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[401.1354675292969,401.1296081542969,401.1314392089844,401.134765625,401.1371154785156,376.5816345214844,401.13775634765625,209.2684783935547,-0.06278431415557861,-0.06278431415557861,-0.06278431415557861,-0.047479934990406036,0.0],"counts":[2772,2731,2811,2689,2803,2203,2355,1232,0,0,0,2,0],"maxs":[402.1717718261533,402.18702154022117,402.1908339687381,402.198458825772,402.17939668318724,402.194646397255,402.1908339687381,402.1908339687381,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,0.0,0.0],"mins":[400.0291869996188,400.02537457110185,400.0291869996188,400.0329994281358,400.0291869996188,0.0,400.0444367136866,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,0.0],"tsAnchor":1636416000,"tsMs":[0,14400000,28800000,43200000,57600000,72000000,86400000,100800000,115200000,129600000,144000000,158400000,172800000,187200000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        res.is_close(&exp)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_sls_archive_wave_1() -> Result<(), Error> {
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-MBF-X:CBM-IN".into(),
        };
        let begstr = "2021-11-09T00:00:00Z";
        let endstr = "2021-11-11T00:10:00Z";
        let res = get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[401.1354675292969,401.1296081542969,401.1314392089844,401.134765625,401.1371154785156,376.5816345214844,401.13775634765625,209.2684783935547,-0.06278431415557861,-0.06278431415557861,-0.06278431415557861,-0.047479934990406036,0.0],"counts":[2772,2731,2811,2689,2803,2203,2355,1232,0,0,0,2,0],"maxs":[402.1717718261533,402.18702154022117,402.1908339687381,402.198458825772,402.17939668318724,402.194646397255,402.1908339687381,402.1908339687381,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,0.0,0.0],"mins":[400.0291869996188,400.02537457110185,400.0291869996188,400.0329994281358,400.0291869996188,0.0,400.0444367136866,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,-0.06278431346925281,0.0],"tsAnchor":1636416000,"tsMs":[0,14400000,28800000,43200000,57600000,72000000,86400000,100800000,115200000,129600000,144000000,158400000,172800000,187200000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        res.is_close(&exp)?;
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn get_sls_archive_wave_2() -> Result<(), Error> {
    let fut = async move {
        let rh = require_sls_test_host_running()?;
        let cluster = &rh.cluster;
        let channel = Channel {
            backend: "sls-archive".into(),
            name: "ARIDI-MBF-X:CBM-IN".into(),
        };
        let begstr = "2021-11-09T10:00:00Z";
        let endstr = "2021-11-10T06:00:00Z";
        let res = get_binned_json_common_res(channel, begstr, endstr, 10, AggKind::TimeWeightedScalar, cluster).await?;
        let exp = r##"{"avgs":[0.00014690558600705117,0.00014207433559931815,0.0001436264137737453,0.00014572929649148136,0.00015340493700932711,0.00014388437557499856,0.00012792187044396996,0.00014416234625969082,0.0001486341789131984,0.000145719779538922],"counts":[209,214,210,219,209,192,171,307,285,232],"maxs":[0.001784245832823217,0.0016909628175199032,0.0017036109929904342,0.0016926786629483104,0.001760474289767444,0.0018568832892924547,0.001740367733873427,0.0017931810580193996,0.0017676990246400237,0.002342566382139921],"mins":[0.000040829672798281536,0.00004028259718324989,0.000037641591916326433,0.000039788486901670694,0.00004028418697998859,0.00003767738598980941,0.0,0.00004095739495824091,0.00004668773908633739,0.00003859612115775235],"tsAnchor":1636452000,"tsMs":[0,7200000,14400000,21600000,28800000,36000000,43200000,50400000,57600000,64800000,72000000],"tsNs":[0,0,0,0,0,0,0,0,0,0,0]}"##;
        let exp: BinnedResponse = serde_json::from_str(exp).unwrap();
        res.is_close(&exp)?;
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

impl BinnedResponse {
    pub fn is_close(&self, other: &Self) -> Result<bool, Error> {
        let reterr = || -> Result<bool, Error> {
            Err(Error::with_msg_no_trace(format!(
                "Mismatch\n{:?}\nVS\n{:?}",
                self, other
            )))
        };
        if self.ts_anchor != other.ts_anchor {
            return reterr();
        }
        if self.finalised_range != other.finalised_range {
            return reterr();
        }
        if self.counts != other.counts {
            return reterr();
        }
        let pairs = [
            (&self.mins, &other.mins),
            (&self.maxs, &other.maxs),
            (&self.avgs, &other.avgs),
        ];
        for (t, u) in pairs {
            for (&a, &b) in t.iter().zip(u) {
                if let (Some(a), Some(b)) = (a, b) {
                    if !f64_close(a, b) {
                        return reterr();
                    }
                } else if let (None, None) = (a, b) {
                } else {
                    return reterr();
                }
            }
        }
        Ok(true)
    }
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
