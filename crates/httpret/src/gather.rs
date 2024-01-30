use crate::body_empty;
use crate::body_string;
use crate::err::Error;
use futures_util::select;
use futures_util::FutureExt;
use http::header;
use http::Method;
use http::StatusCode;
use http::Uri;
use httpclient::connect_client;
use httpclient::http;
use hyper::body::Incoming;
use hyper::Request;
use hyper::Response;
use netpod::log::*;
use netpod::ReqCtx;
use netpod::APP_JSON;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use taskrun::tokio;
use tokio::time::sleep;
use url::Url;

#[derive(Clone, Serialize, Deserialize)]
struct GatherFrom {
    hosts: Vec<GatherHost>,
}

#[derive(Clone, Serialize, Deserialize)]
struct GatherHost {
    host: String,
    port: u16,
    inst: String,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct Tag(pub String);

#[derive(Debug)]
pub struct SubRes<T> {
    pub tag: String,
    pub status: StatusCode,
    pub val: T,
}

pub async fn gather_get_json_generic<SM, NT, FT, OUT>(
    method: http::Method,
    urls: Vec<Url>,
    bodies: Vec<Option<String>>,
    tags: Vec<String>,
    nt: NT,
    ft: FT,
    // TODO use deadline instead.
    // TODO Wait a bit longer compared to remote to receive partial results.
    timeout: Duration,
    ctx: &ReqCtx,
) -> Result<OUT, Error>
where
    SM: Send + 'static,
    NT: Fn(String, Response<Incoming>) -> Pin<Box<dyn Future<Output = Result<SubRes<SM>, Error>> + Send>>
        + Send
        + Sync
        + Copy
        + 'static,
    FT: Fn(Vec<(Tag, Result<SubRes<SM>, Error>)>) -> Result<OUT, Error>,
    SubRes<SM>: fmt::Debug,
{
    // TODO remove magic constant
    let extra_timeout = Duration::from_millis(3000);
    if urls.len() != bodies.len() {
        return Err(Error::with_msg_no_trace(format!("unequal numbers of urls and bodies")));
    }
    if urls.len() != tags.len() {
        return Err(Error::with_msg_no_trace(format!("unequal numbers of urls and tags")));
    }
    let spawned: Vec<_> = urls
        .into_iter()
        .zip(bodies.into_iter())
        .zip(tags.into_iter())
        .filter_map(move |((url, body), tag)| {
            info!("try gather from {}", url);
            let uri: Uri = if let Ok(x) = url.as_str().parse() {
                x
            } else {
                warn!("can not parse {url}");
                return None;
            };
            let req = if body.is_some() {
                if method == Method::GET {
                    warn!("gather sends body via GET");
                }
                Request::builder()
                    .method(method.clone())
                    .header(header::HOST, uri.host().unwrap())
                    .header(http::header::CONTENT_TYPE, APP_JSON)
                    .header(http::header::ACCEPT, APP_JSON)
                    .header(ctx.header_name(), ctx.header_value())
                    .uri(uri)
            } else {
                Request::builder()
                    .method(method.clone())
                    .header(header::HOST, uri.host().unwrap())
                    .header(http::header::CONTENT_TYPE, APP_JSON)
                    .header(http::header::ACCEPT, APP_JSON)
                    .header(ctx.header_name(), ctx.header_value())
                    .uri(uri)
            };
            let body = match body {
                None => body_empty(),
                Some(body) => body_string(body),
            };
            match req.body(body) {
                Ok(req) => {
                    let tag2 = tag.clone();
                    let jh = tokio::spawn(async move {
                        select! {
                            _ = sleep(timeout + extra_timeout).fuse() => {
                                error!("PROXY TIMEOUT");
                                Err(Error::with_msg_no_trace(format!("timeout")))
                            }
                            res = async move {
                                let mut client = match connect_client(req.uri()).await {
                                    Ok(x) => x,
                                    Err(e) => return Err(Error::from_to_string(e)),
                                };
                                let res = match client.send_request(req).await {
                                    Ok(x) => x,
                                    Err(e) => return Err(Error::from_to_string(e)),
                                };
                                Ok(res)
                            }.fuse() => {
                                debug!("received result in time  {res:?}");
                                let ret = nt(tag2, res?).await?;
                                debug!("transformed result in time  {ret:?}");
                                Ok(ret)
                            }
                        }
                    });
                    Some((url, tag, jh))
                }
                Err(e) => {
                    error!("bad request: {e}");
                    None
                }
            }
        })
        .collect();
    let mut a = Vec::new();
    for (_url, tag, jh) in spawned {
        let res = match jh.await {
            Ok(k) => match k {
                Ok(k) => (Tag(tag), Ok(k)),
                Err(e) => {
                    warn!("{e:?}");
                    (Tag(tag), Err(e))
                }
            },
            Err(e) => {
                warn!("{e:?}");
                (Tag(tag), Err(e.into()))
            }
        };
        a.push(res);
    }
    ft(a)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn try_search() {
        let ctx = ReqCtx::for_test();
        let fut = gather_get_json_generic(
            hyper::Method::GET,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            |tag, _res| {
                let fut = async {
                    let ret = SubRes {
                        tag,
                        status: StatusCode::OK,
                        val: (),
                    };
                    Ok(ret)
                };
                Box::pin(fut)
            },
            |_all| Ok(String::from("DUMMY-SEARCH-TEST-RESULT-TODO")),
            Duration::from_millis(800),
            &ctx,
        );
        let _ = fut;
    }
}
