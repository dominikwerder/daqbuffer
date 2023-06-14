use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use chrono::Utc;
use err::Error;
use http::StatusCode;
use hyper::Body;
use items_0::test::f32_iter_cmp_near;
use items_0::test::f64_iter_cmp_near;
use items_0::WithLen;
use items_2::binsdim0::BinsDim0CollectedResult;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::AppendToUrl;
use netpod::Cluster;
use netpod::HostPort;
use netpod::SfDbChannel;
use netpod::APP_JSON;
use query::api4::binned::BinnedQuery;
use serde_json::Value as JsonValue;
use url::Url;

const TEST_BACKEND: &str = "testbackend-00";

fn make_query<S: Into<String>>(
    name: S,
    beg_date: &str,
    end_date: &str,
    bin_count_min: u32,
) -> Result<BinnedQuery, Error> {
    let channel = SfDbChannel::from_name(TEST_BACKEND, name);
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date).into();
    let query = BinnedQuery::new(channel, range, bin_count_min).for_time_weighted_scalar();
    Ok(query)
}

#[test]
fn binned_d0_json_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            SfDbChannel::from_name(TEST_BACKEND, "test-gen-i32-dim0-v01"),
            "1970-01-01T00:20:04.000Z",
            "1970-01-01T00:20:37.000Z",
            6,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        assert_eq!(res.range_final(), true);
        assert_eq!(res.len(), 8);
        assert_eq!(res.ts_anchor_sec(), 1200);
        {
            let a1: Vec<_> = res.ts1_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..8).into_iter().map(|x| 5000 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.ts2_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..8).into_iter().map(|x| 5000 + 5000 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.counts().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..8).into_iter().map(|_| 10).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.mins().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..8).into_iter().map(|x| 2400 + 10 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.maxs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..8).into_iter().map(|x| 2409 + 10 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.avgs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..8).into_iter().map(|x| 2404.5 + 10. * x as f32).collect();
            assert_eq!(f32_iter_cmp_near(a1, a2, 0.01, 0.01), true);
        }
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_01a() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            SfDbChannel::from_name(TEST_BACKEND, "test-gen-i32-dim0-v01"),
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T00:40:30.000Z",
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        assert_eq!(res.range_final(), true);
        assert_eq!(res.len(), 11);
        assert_eq!(res.ts_anchor_sec(), 1200);
        let nb = res.len();
        {
            let a1: Vec<_> = res.ts1_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 120 * 1000 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.ts2_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 120 * 1000 * (1 + x)).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.counts().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|_| 240).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.mins().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 2400 + 240 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.maxs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 2639 + 240 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.avgs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb).into_iter().map(|x| 2520. + 240. * x as f32).collect();
            assert_eq!(f32_iter_cmp_near(a1, a2, 0.001, 0.001), true);
        }
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn binned_d0_json_01b() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = get_binned_json(
            SfDbChannel::from_name(TEST_BACKEND, "test-gen-i32-dim0-v01"),
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T01:20:30.000Z",
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        assert_eq!(res.range_final(), true);
        assert_eq!(res.len(), 13);
        assert_eq!(res.ts_anchor_sec(), 1200);
        let nb = res.len();
        {
            let a1: Vec<_> = res.ts1_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 300 * 1000 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.ts2_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 300 * 1000 * (1 + x)).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.counts().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|_| 600).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.mins().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 2400 + 600 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.maxs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 2999 + 600 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.avgs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb).into_iter().map(|x| 2700. + 600. * x as f32).collect();
            assert_eq!(f32_iter_cmp_near(a1, a2, 0.001, 0.001), true);
        }
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
            SfDbChannel::from_name(TEST_BACKEND, "test-gen-f64-dim1-v00"),
            "1970-01-01T00:20:00Z",
            "1970-01-01T00:20:10Z",
            //"1970-01-01T01:20:45.000Z",
            10,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: BinsDim0CollectedResult<f64> = serde_json::from_value(jsv)?;
        assert_eq!(res.range_final(), true);
        assert_eq!(res.len(), 10);
        assert_eq!(res.ts_anchor_sec(), 1200);
        let nb = res.len();
        {
            let a1: Vec<_> = res.ts1_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 1 * 1000 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.ts2_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 1 * 1000 * (1 + x)).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.counts().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|_| 10).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.mins().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|_| 0.1).collect();
            assert_eq!(f64_iter_cmp_near(a1, a2, 0.05, 0.05), true);
        }
        {
            let a1: Vec<_> = res.maxs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|_| 6.3).collect();
            assert_eq!(f64_iter_cmp_near(a1, a2, 0.05, 0.05), true);
        }
        {
            let a1: Vec<_> = res.avgs().iter().map(|x| *x).collect();
            let a2 = vec![46.2, 40.4, 48.6, 40.6, 45.8, 45.1, 41.1, 48.5, 40.1, 46.8];
            assert_eq!(f32_iter_cmp_near(a1, a2, 0.05, 0.05), true);
        }
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
            SfDbChannel::from_name(TEST_BACKEND, "test-gen-f64-dim1-v00"),
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T01:20:20.000Z",
            2,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: BinsDim0CollectedResult<f64> = serde_json::from_value(jsv)?;
        assert_eq!(res.range_final(), true);
        assert_eq!(res.len(), 4);
        assert_eq!(res.ts_anchor_sec(), 1200);
        let nb = res.len();
        {
            let a1: Vec<_> = res.counts().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|_| 12000).collect();
            assert_eq!(a1, a2);
        }
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
            SfDbChannel::from_name(TEST_BACKEND, "test-gen-i32-dim0-v01"),
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T04:20:30.000Z",
            20,
            cluster,
        )
        .await?;
        debug!("Receveided a response json value: {jsv:?}");
        let res: BinsDim0CollectedResult<i32> = serde_json::from_value(jsv)?;
        assert_eq!(res.range_final(), true);
        assert_eq!(res.len(), 25);
        assert_eq!(res.ts_anchor_sec(), 1200);
        let nb = res.len();
        {
            let a1: Vec<_> = res.ts1_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 600 * 1000 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.ts2_off_ms().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 600 * 1000 * (1 + x)).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.counts().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|_| 1200).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.mins().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 2400 + 1200 * x).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.maxs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 2399 + 1200 * (1 + x)).collect();
            assert_eq!(a1, a2);
        }
        {
            let a1: Vec<_> = res.avgs().iter().map(|x| *x).collect();
            let a2: Vec<_> = (0..nb as _).into_iter().map(|x| 3000. + 1200. * x as f32).collect();
            assert_eq!(f32_iter_cmp_near(a1, a2, 0.001, 0.001), true);
        }
        Ok(())
    };
    taskrun::run(fut)
}

async fn get_binned_json(
    channel: SfDbChannel,
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
    let mut query = BinnedQuery::new(channel, range, bin_count).for_time_weighted_scalar();
    query.merger_out_len_max = Some(240);
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
    debug!("get_binned_json  pretty {pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}
