use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use crate::test::f32_iter_cmp_near;
use chrono::Utc;
use err::Error;
use http::StatusCode;
use hyper::Body;
use items_0::WithLen;
use items_2::eventsdim0::EventsDim0CollectorOutput;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::AppendToUrl;
use netpod::Channel;
use netpod::Cluster;
use netpod::HostPort;
use netpod::APP_JSON;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use url::Url;

fn make_query<S: Into<String>>(name: S, beg_date: &str, end_date: &str) -> Result<PlainEventsQuery, Error> {
    let channel = Channel {
        backend: "test-inmem".into(),
        name: name.into(),
        series: None,
    };
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsQuery::new(channel, range).for_pulse_id_diff();
    Ok(query)
}

#[test]
fn events_plain_json_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let query = make_query("inmem-d0-i32", "1970-01-01T00:20:04.000Z", "1970-01-01T00:21:10.000Z")?;
        let jsv = events_plain_json(query, cluster).await?;
        let res: EventsDim0CollectorOutput<i64> = serde_json::from_value(jsv)?;
        // inmem was meant just for functional test, ignores the requested time range
        assert_eq!(res.ts_anchor_sec(), 1204);
        assert_eq!(res.len(), 66);
        Ok(())
    };
    taskrun::run(fut)
}

// TODO improve by a more information-rich return type.
async fn events_plain_json(query: PlainEventsQuery, cluster: &Cluster) -> Result<JsonValue, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
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
    info!("{pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    info!("time {} ms", ms);
    Ok(res)
}
