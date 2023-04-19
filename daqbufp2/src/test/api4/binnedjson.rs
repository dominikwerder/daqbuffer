use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use crate::test::f32_cmp_near;
use chrono::Utc;
use err::Error;
use http::StatusCode;
use hyper::Body;
use items_0::WithLen;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::AppendToUrl;
use netpod::Channel;
use netpod::Cluster;
use netpod::HostPort;
use netpod::APP_JSON;
use query::api4::binned::BinnedQuery;
use serde_json::Value as JsonValue;
use url::Url;

#[test]
fn binned_d0_json_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:04.000Z",
            "1970-01-01T00:20:37.000Z",
            6,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1200);
        assert_eq!(res.len(), 8);
        assert_eq!(res.ts1_off_ms()[0], 0);
        assert_eq!(res.ts2_off_ms()[0], 5000);
        assert_eq!(res.counts()[0], 5);
        assert_eq!(res.counts()[1], 10);
        assert_eq!(res.counts()[7], 7);
        assert_eq!(res.mins()[0], 2405);
        assert_eq!(res.maxs()[0], 2409);
        assert_eq!(res.mins()[1], 2410);
        assert_eq!(res.maxs()[1], 2419);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_01() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T01:20:30.000Z",
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1200);
        assert_eq!(res.len(), 13);
        assert_eq!(res.range_final(), true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_02() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "wave-f64-be-n21".into(),
                series: None,
            },
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T01:20:45.000Z",
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<f64> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1200);
        assert_eq!(res.len(), 13);
        assert_eq!(res.range_final(), true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_03() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "wave-f64-be-n21".into(),
                series: None,
            },
            // TODO This test was meant to ask `AggKind::DimXBinsN(3)`
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T01:20:20.000Z",
            2,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<f64> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1200);
        assert_eq!(res.len(), 4);
        assert_eq!(res.range_final(), true);
        assert_eq!(res.counts()[0], 300);
        assert_eq!(res.counts()[3], 8);
        assert_eq!(f32_cmp_near(res.avgs()[0], 44950.00390625), true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_04() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "const-regular-scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T04:20:30.000Z",
            // TODO must use AggKind::DimXBins1
            20,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1200);
        assert_eq!(res.len(), 17);
        // TODO I would expect rangeFinal to be set, or?
        assert_eq!(res.range_final(), false);
        assert_eq!(f32_cmp_near(res.avgs()[0], 42.0), true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_05() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "const-regular-scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T10:20:30.000Z",
            // TODO must use AggKind::DimXBins1
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 0);
        // TODO make disk parse faster and avoid timeout
        assert_eq!(res.len(), 11);
        assert_eq!(res.range_final(), false);
        assert_eq!(f32_cmp_near(res.avgs()[0], 42.0), true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_06() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "const-regular-scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T00:20:20.000Z",
            // TODO must use AggKind::TimeWeightedScalar
            20,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1210);
        assert_eq!(res.len(), 20);
        assert_eq!(res.range_final(), true);
        assert_eq!(f32_cmp_near(res.avgs()[0], 42.0), true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_07() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "const-regular-scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:11.000Z",
            "1970-01-01T00:30:20.000Z",
            // TODO must use AggKind::TimeWeightedScalar
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1200);
        assert_eq!(res.len(), 11);
        assert_eq!(res.range_final(), true);
        assert_eq!(f32_cmp_near(res.avgs()[0], 42.0), true);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_inmem_d0_json_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "const-regular-scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:11.000Z",
            "1970-01-01T00:30:20.000Z",
            // TODO must use AggKind::TimeWeightedScalar
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: items_2::binsdim0::BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1200);
        assert_eq!(res.len(), 11);
        assert_eq!(res.range_final(), true);
        assert_eq!(f32_cmp_near(res.avgs()[0], 42.0), true);
        Ok(())
    };
    taskrun::run(fut)
}

async fn get_binned_json(
    channel: Channel,
    beg_date: &str,
    end_date: &str,
    bin_count: u32,
    cluster: &Cluster,
) -> Result<JsonValue, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date).into();
    let query = BinnedQuery::new(channel, range, bin_count).for_time_weighted_scalar();
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("http get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_JSON)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("error response {:?}", res);
        let buf = hyper::body::to_bytes(res.into_body()).await.ec()?;
        let s = String::from_utf8_lossy(&buf);
        error!("body of error response: {s}");
        return Err(Error::with_msg_no_trace(format!("error response")));
    }
    let buf = hyper::body::to_bytes(res.into_body()).await.ec()?;
    let s = String::from_utf8_lossy(&buf);
    let res: JsonValue = serde_json::from_str(&s)?;
    let pretty = serde_json::to_string_pretty(&res)?;
    trace!("{pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}
