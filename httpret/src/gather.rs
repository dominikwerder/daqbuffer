use crate::response;
use err::Error;
use http::{Method, StatusCode};
use hyper::{Body, Client, Request, Response};
use netpod::{Node, NodeConfigCached};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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

pub async fn gather_json_from_hosts(req: Request<Body>, pathpre: &str) -> Result<Response<Body>, Error> {
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
        use futures_util::select;
        use futures_util::FutureExt;
        use std::time::Duration;
        use tokio::time::sleep;
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
            let req = req.header("x-node-from-name", format!("{}", node_config.node_config.name));
            let req = req.header(http::header::ACCEPT, "application/json");
            let req = req.body(Body::empty());
            use futures_util::select;
            use futures_util::FutureExt;
            use std::time::Duration;
            use tokio::time::sleep;
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
