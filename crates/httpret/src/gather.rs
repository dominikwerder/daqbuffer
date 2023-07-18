use crate::response;
use crate::RetrievalError;
use futures_util::select;
use futures_util::FutureExt;
use http::Method;
use http::StatusCode;
use hyper::Body;
use hyper::Client;
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

async fn process_answer(res: Response<Body>) -> Result<JsonValue, RetrievalError> {
    let (pre, mut body) = res.into_parts();
    if pre.status != StatusCode::OK {
        use hyper::body::HttpBody;
        if let Some(c) = body.data().await {
            let c: bytes::Bytes = c?;
            let s1 = String::from_utf8(c.to_vec())?;
            Ok(JsonValue::String(format!(
                "status {}  body {}",
                pre.status.as_str(),
                s1
            )))
        } else {
            Ok(JsonValue::String(format!("status {}", pre.status.as_str())))
        }
    } else {
        let body: hyper::Body = body;
        let body_all = hyper::body::to_bytes(body).await?;
        let val = match serde_json::from_slice(&body_all) {
            Ok(k) => k,
            Err(_e) => JsonValue::String(String::from_utf8(body_all.to_vec())?),
        };
        Ok::<_, RetrievalError>(val)
    }
}

pub async fn unused_gather_json_from_hosts(
    req: Request<Body>,
    pathpre: &str,
) -> Result<Response<Body>, RetrievalError> {
    let (part_head, part_body) = req.into_parts();
    let bodyslice = hyper::body::to_bytes(part_body).await?;
    let gather_from: GatherFrom = serde_json::from_slice(&bodyslice)?;
    let mut spawned = vec![];
    let uri = part_head.uri;
    let path_post = &uri.path()[pathpre.len()..];
    for gh in gather_from.hosts {
        let uri = format!("http://{}:{}/{}", gh.host, gh.port, path_post);
        let req = Request::builder().method(Method::GET).uri(uri);
        let req = if gh.inst.len() > 0 {
            req.header("retrieval_instance", &gh.inst)
        } else {
            req
        };
        let req = req.header(http::header::ACCEPT, APP_JSON);
        let req = req.body(Body::empty());
        let task = tokio::spawn(async move {
            select! {
              _ = sleep(Duration::from_millis(1500)).fuse() => {
                Err(RetrievalError::with_msg("timeout"))
              }
              res = Client::new().request(req?).fuse() => Ok(process_answer(res?).await?)
            }
        });
        spawned.push((gh.clone(), task));
    }
    #[derive(Serialize)]
    struct Hres {
        gh: GatherHost,
        res: JsonValue,
    }
    #[derive(Serialize)]
    struct Jres {
        hosts: Vec<Hres>,
    }
    let mut a = vec![];
    for tr in spawned {
        let res = match tr.1.await {
            Ok(k) => match k {
                Ok(k) => k,
                Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
            },
            Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
        };
        a.push(Hres { gh: tr.0, res });
    }
    let res = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON)
        .body(serde_json::to_string(&Jres { hosts: a })?.into())?;
    Ok(res)
}

pub async fn gather_get_json(
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    let (head, body) = req.into_parts();
    let _bodyslice = hyper::body::to_bytes(body).await?;
    let pathpre = "/api/4/gather/";
    let pathsuf = &head.uri.path()[pathpre.len()..];
    let spawned: Vec<_> = node_config
        .node_config
        .cluster
        .nodes
        .iter()
        .map(|node| {
            let uri = format!("http://{}:{}/api/4/{}", node.host, node.port, pathsuf);
            let req = Request::builder().method(Method::GET).uri(uri);
            let req = req.header(http::header::ACCEPT, APP_JSON);
            let req = req.body(Body::empty());
            let task = tokio::spawn(async move {
                select! {
                  _ = sleep(Duration::from_millis(1500)).fuse() => {
                    Err(RetrievalError::with_msg("timeout"))
                  }
                  res = Client::new().request(req?).fuse() => Ok(process_answer(res?).await?)
                }
            });
            (node.clone(), task)
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
    let mut a = vec![];
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
    bodies: Vec<Option<Body>>,
    tags: Vec<String>,
    nt: NT,
    ft: FT,
    // TODO use deadline instead.
    // TODO Wait a bit longer compared to remote to receive partial results.
    timeout: Duration,
) -> Result<OUT, RetrievalError>
where
    SM: Send + 'static,
    NT: Fn(String, Response<Body>) -> Pin<Box<dyn Future<Output = Result<SubRes<SM>, RetrievalError>> + Send>>
        + Send
        + Sync
        + Copy
        + 'static,
    FT: Fn(Vec<(Tag, Result<SubRes<SM>, RetrievalError>)>) -> Result<OUT, RetrievalError>,
{
    // TODO remove magic constant
    let extra_timeout = Duration::from_millis(3000);
    if urls.len() != bodies.len() {
        return Err(RetrievalError::TextError(format!("unequal numbers of urls and bodies")));
    }
    if urls.len() != tags.len() {
        return Err(RetrievalError::TextError(format!("unequal numbers of urls and tags")));
    }
    let spawned: Vec<_> = urls
        .into_iter()
        .zip(bodies.into_iter())
        .zip(tags.into_iter())
        .map(move |((url, body), tag)| {
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
                None => Body::empty(),
                Some(body) => body,
            };
            let req = req.body(body);
            let tag2 = tag.clone();
            let jh = tokio::spawn(async move {
                select! {
                    _ = sleep(timeout + extra_timeout).fuse() => {
                        error!("PROXY TIMEOUT");
                        Err(RetrievalError::TextError(format!("timeout")))
                    }
                    res = {
                        let client = Client::new();
                        client.request(req?).fuse()
                    } => {
                        info!("received result in time");
                        let ret = nt(tag2, res?).await?;
                        info!("transformed result in time");
                        Ok(ret)
                    }
                }
            });
            (url, tag, jh)
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
