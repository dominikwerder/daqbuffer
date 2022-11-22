use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use err::Error;
use futures_util::Future;
use http::{header, Request, StatusCode};
use httpclient::{http_get, http_post};
use hyper::Body;
use netpod::log::*;
use netpod::query::api1::{Api1Query, Api1Range};
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
        let accept = "application/json";
        //let qu = Api1Query::new(Api1Range::new(), vec!["testbackend/scalar-i32-be"]);
        let buf = http_post(url, accept, "{}".into()).await?;
        let js = String::from_utf8_lossy(&buf);
        eprintln!("string received: {js}");
        Ok(())
    };
    testrun(fut)?;
    Ok(())
}
