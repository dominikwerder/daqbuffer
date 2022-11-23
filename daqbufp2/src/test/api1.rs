use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use chrono::{DateTime, Utc};
use err::Error;
use futures_util::Future;
use http::{header, Request, StatusCode};
use httpclient::{http_get, http_post};
use hyper::Body;
use netpod::log::*;
use netpod::query::api1::{Api1Query, Api1Range, ChannelTuple};
use url::Url;

fn testrun<T, F>(fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    taskrun::run(fut)
}

#[test]
fn events_f64_plain() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let node = &cluster.nodes[0];
        let url: Url = format!("http://{}:{}/api/1/query", node.host, node.port).parse()?;
        let accept = "application/octet-stream";
        let range = Api1Range::new("1970-01-01T00:00:00Z".try_into()?, "1970-01-01T00:01:00Z".try_into()?)?;
        // TODO the channel list needs to get pre-processed to check for backend prefix!
        let ch = ChannelTuple::new("test-disk-databuffer".into(), "scalar-i32-be".into());
        let qu = Api1Query::new(range, vec![ch]);
        let body = serde_json::to_string(&qu)?;
        let buf = http_post(url, accept, body.into()).await?;
        eprintln!("body received: {}", buf.len());
        //let js = String::from_utf8_lossy(&buf);
        //eprintln!("string received: {js}");
        Ok(())
    };
    testrun(fut)?;
    Ok(())
}
