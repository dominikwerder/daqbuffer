use crate::response;
use err::Error;
use http::{HeaderValue, StatusCode};
use hyper::{Body, Request, Response};
use netpod::{ChannelSearchQuery, ProxyConfig};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub async fn channel_search(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get("accept") {
        Some(v) => {
            if v == "application/json" {
                // TODO actually pass on the query parameters to the sub query.
                err::todo();
                let query = ChannelSearchQuery::from_request(head.uri.query())?;
                let uri = format!("/api/4/search/channel");
                let nt = |_res| {
                    let fut = async { Ok(0f32) };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = Result<f32, Error>> + Send>>
                };
                let ft = |_all| {
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(serde_json::to_string(&42)?.into())?;
                    Ok(res)
                };
                let mut ret = crate::gather::gather_get_json_generic::<f32, _, _>(
                    http::Method::GET,
                    uri,
                    proxy_config.search_hosts.clone(),
                    nt,
                    ft,
                    Duration::from_millis(3000),
                )
                .await?;
                ret.headers_mut()
                    .append("x-proxy-log-mark", HeaderValue::from_str("proxied")?);
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        _ => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}
