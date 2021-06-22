use crate::response;
use err::Error;
use futures_util::{select, FutureExt};
use http::{Method, StatusCode};
use hyper::{Body, Client, Request, Response};
use hyper_tls::HttpsConnector;
use netpod::{Node, NodeConfigCached, APP_JSON};
use serde::{Deserialize, Serialize};
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

async fn process_answer(res: Response<Body>) -> Result<JsonValue, Error> {
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
        Ok::<_, Error>(val)
    }
}

pub async fn unused_gather_json_from_hosts(req: Request<Body>, pathpre: &str) -> Result<Response<Body>, Error> {
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
        let req = req.header(http::header::ACCEPT, "application/json");
        let req = req.body(Body::empty());
        let task = tokio::spawn(async move {
            select! {
              _ = sleep(Duration::from_millis(1500)).fuse() => {
                Err(Error::with_msg("timeout"))
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
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&Jres { hosts: a })?.into())?;
    Ok(res)
}

pub async fn gather_get_json(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
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
            let req = req.header("x-log-from-node-name", format!("{}", node_config.node_config.name));
            let req = req.header(http::header::ACCEPT, "application/json");
            let req = req.body(Body::empty());
            let task = tokio::spawn(async move {
                select! {
                  _ = sleep(Duration::from_millis(1500)).fuse() => {
                    Err(Error::with_msg("timeout"))
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
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&Jres { hosts: a })?.into())?;
    Ok(res)
}

pub struct SubRes<T> {
    pub tag: String,
    pub val: T,
}

pub async fn gather_get_json_generic<SM, NT, FT>(
    method: http::Method,
    urls: Vec<Url>,
    bodies: Option<Vec<Body>>,
    tags: Vec<String>,
    nt: NT,
    ft: FT,
    timeout: Duration,
) -> Result<Response<Body>, Error>
where
    SM: Send + 'static,
    NT: Fn(Response<Body>) -> Pin<Box<dyn Future<Output = Result<SM, Error>> + Send>> + Send + Sync + Copy + 'static,
    FT: Fn(Vec<SubRes<SM>>) -> Result<Response<Body>, Error>,
{
    assert!(urls.len() == tags.len());
    let bodies: Vec<_> = match bodies {
        None => (0..urls.len()).into_iter().map(|_| Body::empty()).collect(),
        Some(bodies) => bodies,
    };
    let spawned: Vec<_> = urls
        .into_iter()
        .zip(bodies.into_iter())
        .zip(tags.into_iter())
        .map(move |((url, body), tag)| {
            let url_str = url.as_str();
            let is_tls = if url_str.starts_with("https://") { true } else { false };
            let req = Request::builder().method(method.clone()).uri(url_str);
            //let req = req.header("x-log-from-node-name", format!("{}", node_config.node_config.name));
            let req = req.header(http::header::ACCEPT, APP_JSON);
            let req = req.body(body);
            let task = tokio::spawn(async move {
                select! {
                    _ = sleep(timeout).fuse() => {
                        Err(Error::with_msg("timeout"))
                    }
                    res = {
                        if is_tls {
                            let https = HttpsConnector::new();
                            let client = Client::builder().build::<_, hyper::Body>(https);
                            client.request(req?).fuse()
                        }
                        else {
                            let client = Client::new();
                            client.request(req?).fuse()
                        }
                    } => {
                        let ret = SubRes {
                            tag: tag,
                            val:nt(res?).await?,
                        };
                        Ok(ret)
                    }
                }
            });
            (url, task)
        })
        .collect();
    let mut a = vec![];
    for (_schemehostport, jh) in spawned {
        let res = match jh.await {
            Ok(k) => match k {
                Ok(k) => k,
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e.into()),
        };
        a.push(res);
    }
    let a = a;
    ft(a)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn try_search() {
        let fut = gather_get_json_generic(
            hyper::Method::GET,
            vec![],
            None,
            vec![],
            |_res| {
                let fut = async { Ok(()) };
                Box::pin(fut)
            },
            |_all| {
                let res = response(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_string(&42)?.into())?;
                Ok(res)
            },
            Duration::from_millis(4000),
        );
        let _ = fut;
    }
}
