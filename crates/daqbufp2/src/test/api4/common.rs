use crate::err::ErrConv;
use chrono::Utc;
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::Cluster;
use netpod::HostPort;
use netpod::APP_JSON;
use query::api4::binned::BinnedQuery;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use url::Url;

// TODO improve by a more information-rich return type.
pub async fn fetch_events_json(query: PlainEventsQuery, cluster: &Cluster) -> Result<JsonValue, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/events", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("fetch_events_json  url {}", url);
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
    debug!("fetch_binned_json pretty: {pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}

// TODO improve by a more information-rich return type.
pub async fn fetch_binned_json(query: BinnedQuery, cluster: &Cluster) -> Result<JsonValue, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("fetch_binned_json  url {}", url);
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
    debug!("fetch_binned_json pretty: {pretty}");
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}
