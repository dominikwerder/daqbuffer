use crate::body_empty;
use crate::body_string;
use crate::err::Error;
use crate::response;
use crate::Requ;
use crate::RespFull;
use futures_util::select;
use futures_util::FutureExt;
use http::Method;
use http::StatusCode;
use httpclient::connect_client;
use httpclient::read_body_bytes;
use hyper::body::Incoming;
use hyper::Request;
use hyper::Response;
use netpod::log::*;
use netpod::Node;
use netpod::NodeConfigCached;
use netpod::APP_JSON;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
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

async fn process_answer(res: Response<hyper::body::Incoming>) -> Result<JsonValue, Error> {
    let (pre, body) = res.into_parts();
    if pre.status != StatusCode::OK {
        let buf = read_body_bytes(body).await?;
        let s = String::from_utf8(buf.to_vec())?;
        Ok(JsonValue::String(format!("status {}  body {}", pre.status.as_str(), s)))
    } else {
        let body_all = read_body_bytes(body).await?;
        let val = match serde_json::from_slice(&body_all) {
            Ok(k) => k,
            Err(_e) => JsonValue::String(String::from_utf8(body_all.to_vec())?),
        };
        Ok::<_, Error>(val)
    }
}

pub async fn gather_get_json(req: Requ, node_config: &NodeConfigCached) -> Result<RespFull, Error> {
    let (head, body) = req.into_parts();
    let _bodyslice = read_body_bytes(body).await?;
    let pathpre = "/api/4/gather/";
    let pathsuf = &head.uri.path()[pathpre.len()..];
    let spawned: Vec<_> = node_config
        .node_config
        .cluster
        .nodes
        .iter()
        .filter_map(|node| {
            let uri = format!("http://{}:{}/api/4/{}", node.host, node.port, pathsuf);
            let req = Request::builder().method(Method::GET).uri(uri);
            let req = req.header(http::header::ACCEPT, APP_JSON);
            match req.body(body_empty()) {
                Ok(req) => {
                    let task = tokio::spawn(async move {
                        select! {
                          _ = sleep(Duration::from_millis(1500)).fuse() => {
                            Err(Error::with_msg_no_trace(format!("timeout")))
                            }
                            res = async move {
                                let mut client = if let Ok(x) = connect_client(req.uri()).await {x}
                                else { return Err(Error::with_msg("can not make request")); };
                                let res = if let Ok(x) = client.send_request(req).await { x }
                                else { return Err(Error::with_msg("can not make request")); };
                                Ok(res)
                            }.fuse() => {
                                Ok(process_answer(res?).await?)
                            }
                        }
                    });
                    Some((node.clone(), task))
                }
                Err(e) => {
                    error!("bad request: {e}");
                    None
                }
            }
        })
        .collect();
    #[derive(Serialize)]
    struct Hres {
        node: Node,
        res: JsonValue,
    }
    #[derive(Serialize)]
    struct Jres {
        hosts: Vec<Hres>,
    }
    let mut a = Vec::new();
    for (node, jh) in spawned {
        let res = match jh.await {
            Ok(k) => match k {
                Ok(k) => k,
                Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
            },
            Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
        };
        let v = Hres {
            node: node.clone(),
            res,
        };
        a.push(v);
    }
    let a = a;
    let res = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON)
        .body(serde_json::to_string(&Jres { hosts: a })?.into())?;
    Ok(res)
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct Tag(pub String);

pub struct SubRes<T> {
    pub tag: String,
    pub status: StatusCode,
    pub val: T,
}

pub async fn gather_get_json_generic<SM, NT, FT, OUT>(
    _method: http::Method,
    urls: Vec<Url>,
    bodies: Vec<Option<String>>,
    tags: Vec<String>,
    nt: NT,
    ft: FT,
    // TODO use deadline instead.
    // TODO Wait a bit longer compared to remote to receive partial results.
    timeout: Duration,
) -> Result<OUT, Error>
where
    SM: Send + 'static,
    NT: Fn(String, Response<Incoming>) -> Pin<Box<dyn Future<Output = Result<SubRes<SM>, Error>> + Send>>
        + Send
        + Sync
        + Copy
        + 'static,
    FT: Fn(Vec<(Tag, Result<SubRes<SM>, Error>)>) -> Result<OUT, Error>,
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
            info!("Try gather from {}", url);
            let url_str = url.as_str();
            let req = if body.is_some() {
                Request::builder().method(Method::POST).uri(url_str)
            } else {
                Request::builder().method(Method::GET).uri(url_str)
            };
            let req = req.header(http::header::ACCEPT, APP_JSON);
            let req = if body.is_some() {
                req.header(http::header::CONTENT_TYPE, APP_JSON)
            } else {
                req
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
                                info!("received result in time");
                                let ret = nt(tag2, res?).await?;
                                info!("transformed result in time");
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
        );
        let _ = fut;
    }
}
