use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use chrono::Utc;
use err::Error;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::Cluster;
use netpod::HostPort;
use netpod::SfDbChannel;
use netpod::APP_JSON;
use url::Url;

const TEST_BACKEND: &str = "testbackend-00";

// Fetches all data, not streaming, meant for basic test cases that fit in memory.
async fn fetch_data_api_python_blob(
    channels: Vec<SfDbChannel>,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
) -> Result<Vec<u8>, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date = beg_date.parse()?;
    let end_date = end_date.parse()?;
    let _range = NanoRange::from_date_time(beg_date, end_date);
    let start_date = beg_date.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let end_date = end_date.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let query = serde_json::json!({
        "range": {
            "type": "date",
            "startDate": start_date,
            "endDate": end_date,
        },
        "channels": channels.iter().map(|x| x.name()).collect::<Vec<_>>(),
    });
    let query_str = serde_json::to_string_pretty(&query)?;
    let hp = HostPort::from_node(node0);
    let url = Url::parse(&format!("http://{}:{}/api/1/query", hp.host, hp.port))?;
    info!("http get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::POST)
        .uri(url.to_string())
        .header(http::header::CONTENT_TYPE, APP_JSON)
        //.header(http::header::ACCEPT, APP_JSON)
        .body(Body::from(query_str))
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
        return Err(Error::with_msg_no_trace(format!("bad result {res:?}")));
    }
    let buf = hyper::body::to_bytes(res.into_body()).await.ec()?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    info!("time {} ms  body len {}", ms, buf.len());
    Ok(buf.into())
}

#[test]
fn api3_hdf_dim0_00() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let jsv = fetch_data_api_python_blob(
            vec![SfDbChannel::from_name(TEST_BACKEND, "test-gen-i32-dim0-v00")],
            "1970-01-01T00:20:04.000Z",
            "1970-01-01T00:21:10.000Z",
            cluster,
        )
        .await?;
        Ok(())
    };
    taskrun::run(fut)
}
