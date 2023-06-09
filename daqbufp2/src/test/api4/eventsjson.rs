use std::time::Duration;

use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use crate::test::f32_iter_cmp_near;
use chrono::{DateTime, Utc};
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::query::PlainEventsQuery;
use netpod::APP_JSON;
use netpod::{log::*, AggKind};
use netpod::{AppendToUrl, Channel, Cluster, HostPort, NanoRange};
use serde_json::Value as JsonValue;
use url::Url;

#[test]
fn events_plain_json_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = events_plain_json(
            Channel {
                backend: "test-inmem".into(),
                name: "inmem-d0-i32".into(),
                series: None,
            },
            "1970-01-01T00:20:04.000Z",
            "1970-01-01T00:20:10.000Z",
            cluster,
        )
        .await?;
        info!("Receveided a response json value: {jsv:?}");
        let res: items_2::eventsdim0::EventsDim0CollectorOutput<i32> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 0);
        assert_eq!(res.len(), 60);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn events_plain_json_01() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = events_plain_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "scalar-i32-be".into(),
                series: None,
            },
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T00:20:13.000Z",
            cluster,
        )
        .await?;
        info!("Receveided a response json value: {jsv:?}");
        let res: items_2::eventsdim0::EventsDim0CollectorOutput<i32> = serde_json::from_value(jsv)?;
        assert_eq!(res.ts_anchor_sec(), 1210);
        assert_eq!(res.pulse_anchor(), 2420);
        let exp = [2420., 2421., 2422., 2423., 2424., 2425.];
        assert_eq!(f32_iter_cmp_near(res.values_to_f32(), exp), true);
        assert_eq!(res.range_complete(), true);
        assert_eq!(res.timed_out(), false);
        Ok(())
    };
    taskrun::run(fut)
}

#[test]
fn events_plain_json_02_range_incomplete() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = events_plain_json(
            Channel {
                backend: "test-disk-databuffer".into(),
                name: "scalar-i32-be".into(),
                series: None,
            },
            "1970-01-03T23:59:55.000Z",
            "1970-01-04T00:00:01.000Z",
            cluster,
        )
        .await?;
        let res: items_2::eventsdim0::EventsDim0CollectorOutput<i32> = serde_json::from_value(jsv).unwrap();
        assert_eq!(res.range_complete(), false);
        assert_eq!(res.timed_out(), false);
        Ok(())
    };
    taskrun::run(fut)
}

// TODO improve by a more information-rich return type.
async fn events_plain_json(
    channel: Channel,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
) -> Result<JsonValue, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsQuery::new(
        channel,
        range,
        AggKind::TimeWeightedScalar,
        Duration::from_millis(10000),
        None,
        true,
    );
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/events", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    info!("http get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_JSON)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
        return Err(Error::with_msg_no_trace(format!("bad result {res:?}")));
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
