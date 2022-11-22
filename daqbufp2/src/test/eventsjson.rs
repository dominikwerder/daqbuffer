use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use chrono::{DateTime, Utc};
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::APP_JSON;
use netpod::{AppendToUrl, Channel, Cluster, HostPort, NanoRange};
use serde_json::Value as JsonValue;
use url::Url;

#[test]
fn events_plain_json_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        events_plain_json(
            Channel {
                backend: "testbackend".into(),
                name: "inmem-d0-i32".into(),
                series: None,
            },
            "1970-01-01T00:20:04.000Z",
            "1970-01-01T00:20:10.000Z",
            cluster,
            true,
            4,
        )
        .await?;
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
    _expect_range_complete: bool,
    _expect_event_count: u64,
) -> Result<JsonValue, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsQuery::new(channel, range, 1024 * 4, None, false);
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/events", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    info!("get_plain_events  get {}", url);
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
    eprintln!("{pretty}");

    // TODO assert more
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}
