use crate::gather::{gather_get_json_generic, SubRes};
use crate::response;
use err::Error;
use http::{Method, StatusCode};
use hyper::{Body, Client, Request, Response};
use itertools::Itertools;
use netpod::{ChannelSearchQuery, ChannelSearchResult, ProxyConfig, APP_JSON};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use url::Url;

pub trait BackendAware {
    fn backend(&self) -> &str;
}

pub trait FromErrorCode {
    fn from_error_code(backend: &str, code: ErrorCode) -> Self;
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ErrorCode {
    Error,
    Timeout,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrorDescription {
    code: ErrorCode,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Ordering {
    #[serde(rename = "none")]
    NONE,
    #[serde(rename = "asc")]
    ASC,
    #[serde(rename = "desc")]
    DESC,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelSearchQueryV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    #[serde(rename = "sourceRegex", skip_serializing_if = "Option::is_none")]
    pub source_regex: Option<String>,
    #[serde(rename = "descriptionRegex", skip_serializing_if = "Option::is_none")]
    pub description_regex: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub backends: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering: Option<Ordering>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchResultItemV1 {
    pub backend: String,
    pub channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDescription>,
}

impl BackendAware for ChannelSearchResultItemV1 {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl FromErrorCode for ChannelSearchResultItemV1 {
    fn from_error_code(backend: &str, code: ErrorCode) -> Self {
        Self {
            backend: backend.into(),
            channels: vec![],
            error: Some(ErrorDescription { code }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchResultV1(pub Vec<ChannelSearchResultItemV1>);

pub async fn channel_search_list_v1(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, reqbody) = req.into_parts();
    let bodybytes = hyper::body::to_bytes(reqbody).await?;
    let query: ChannelSearchQueryV1 = serde_json::from_slice(&bodybytes)?;
    match head.headers.get("accept") {
        Some(v) => {
            if v == APP_JSON {
                let query = ChannelSearchQuery {
                    name_regex: query.regex.map_or(String::new(), |k| k),
                    source_regex: query.source_regex.map_or(String::new(), |k| k),
                    description_regex: query.description_regex.map_or(String::new(), |k| k),
                };
                let urls = proxy_config
                    .search_hosts
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(e) => Err(Error::with_msg(format!("parse error for: {:?}  {:?}", sh, e))),
                    })
                    .fold_ok(vec![], |mut a, x| {
                        a.push(x);
                        a
                    })?;
                let tags: Vec<_> = urls.iter().map(|k| k.to_string()).collect();
                let nt = |res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<SubRes<ChannelSearchResult>>| {
                    let mut res = ChannelSearchResultV1(vec![]);
                    for j in all {
                        for k in j.val.channels {
                            let mut found = false;
                            let mut i2 = 0;
                            for i1 in 0..res.0.len() {
                                if res.0[i1].backend == k.backend {
                                    found = true;
                                    i2 = i1;
                                    break;
                                }
                            }
                            if !found {
                                let u = ChannelSearchResultItemV1 {
                                    backend: k.backend,
                                    channels: vec![],
                                    error: None,
                                };
                                res.0.push(u);
                                i2 = res.0.len() - 1;
                            }
                            res.0[i2].channels.push(k.name);
                        }
                    }
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, APP_JSON)
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                let bodies = (0..urls.len()).into_iter().map(|_| None).collect();
                let ret = gather_get_json_generic(
                    http::Method::GET,
                    urls,
                    bodies,
                    tags,
                    nt,
                    ft,
                    Duration::from_millis(3000),
                )
                .await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

pub async fn channel_search_configs_v1(
    req: Request<Body>,
    proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error> {
    let (head, reqbody) = req.into_parts();
    let bodybytes = hyper::body::to_bytes(reqbody).await?;
    let query: ChannelSearchQueryV1 = serde_json::from_slice(&bodybytes)?;
    match head.headers.get("accept") {
        Some(v) => {
            if v == APP_JSON {
                // Transform the ChannelSearchQueryV1 to ChannelSearchQuery
                let query = ChannelSearchQuery {
                    name_regex: query.regex.map_or(String::new(), |k| k),
                    source_regex: query.source_regex.map_or(String::new(), |k| k),
                    description_regex: query.description_regex.map_or(String::new(), |k| k),
                };
                let urls = proxy_config
                    .search_hosts
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(e) => Err(Error::with_msg(format!("parse error for: {:?}  {:?}", sh, e))),
                    })
                    .fold_ok(vec![], |mut a, x| {
                        a.push(x);
                        a
                    })?;
                let tags: Vec<_> = urls.iter().map(|k| k.to_string()).collect();
                let nt = |res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<SubRes<ChannelSearchResult>>| {
                    let mut res = ChannelConfigsResponseV1(vec![]);
                    for j in all {
                        for k in j.val.channels {
                            let mut found = false;
                            let mut i2 = 0;
                            for i1 in 0..res.0.len() {
                                if res.0[i1].backend == k.backend {
                                    found = true;
                                    i2 = i1;
                                    break;
                                }
                            }
                            if !found {
                                let u = ChannelBackendConfigsV1 {
                                    backend: k.backend.clone(),
                                    channels: vec![],
                                    error: None,
                                };
                                res.0.push(u);
                                i2 = res.0.len() - 1;
                            }
                            {
                                let shape = if k.shape.len() == 0 { None } else { Some(k.shape) };
                                let unit = if k.unit.len() == 0 { None } else { Some(k.unit) };
                                let description = if k.description.len() == 0 {
                                    None
                                } else {
                                    Some(k.description)
                                };
                                let t = ChannelConfigV1 {
                                    backend: k.backend,
                                    name: k.name,
                                    source: k.source,
                                    description,
                                    ty: k.ty,
                                    shape,
                                    unit,
                                };
                                res.0[i2].channels.push(t);
                            }
                        }
                    }
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, APP_JSON)
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                let bodies = (0..urls.len()).into_iter().map(|_| None).collect();
                let ret = gather_get_json_generic(
                    http::Method::GET,
                    urls,
                    bodies,
                    tags,
                    nt,
                    ft,
                    Duration::from_millis(3000),
                )
                .await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigV1 {
    pub backend: String,
    pub name: String,
    pub source: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shape: Option<Vec<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigsQueryV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    #[serde(rename = "sourceRegex")]
    pub source_regex: Option<String>,
    #[serde(rename = "descriptionRegex")]
    pub description_regex: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub backends: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering: Option<Ordering>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelBackendConfigsV1 {
    pub backend: String,
    pub channels: Vec<ChannelConfigV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDescription>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigsResponseV1(pub Vec<ChannelBackendConfigsV1>);

impl BackendAware for ChannelBackendConfigsV1 {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl FromErrorCode for ChannelBackendConfigsV1 {
    fn from_error_code(backend: &str, code: ErrorCode) -> Self {
        Self {
            backend: backend.into(),
            channels: vec![],
            error: Some(ErrorDescription { code }),
        }
    }
}

// TODO replace usage of this by gather-generic
pub async fn gather_json_2_v1(
    req: Request<Body>,
    pathpre: &str,
    _proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error> {
    let (part_head, part_body) = req.into_parts();
    let bodyslice = hyper::body::to_bytes(part_body).await?;
    let gather_from: GatherFromV1 = serde_json::from_slice(&bodyslice)?;
    let mut spawned = vec![];
    let uri = part_head.uri;
    let path_post = &uri.path()[pathpre.len()..];
    //let hds = part_head.headers;
    for gh in gather_from.hosts {
        let uri = format!("http://{}:{}/{}", gh.host, gh.port, path_post);
        let req = Request::builder().method(Method::GET).uri(uri);
        let req = if gh.inst.len() > 0 {
            req.header("retrieval_instance", &gh.inst)
        } else {
            req
        };
        let req = req.header(http::header::ACCEPT, APP_JSON);
        //.body(Body::from(serde_json::to_string(&q)?))?;
        let req = req.body(Body::empty());
        let task = tokio::spawn(async move {
            //let res = Client::new().request(req);
            let res = Client::new().request(req?).await;
            Ok::<_, Error>(process_answer(res?).await?)
        });
        let task = tokio::time::timeout(std::time::Duration::from_millis(5000), task);
        spawned.push((gh.clone(), task));
    }
    #[derive(Serialize)]
    struct Hres {
        gh: GatherHostV1,
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
                Ok(k) => match k {
                    Ok(k) => k,
                    Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
                },
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

#[derive(Clone, Serialize, Deserialize)]
struct GatherFromV1 {
    hosts: Vec<GatherHostV1>,
}

#[derive(Clone, Serialize, Deserialize)]
struct GatherHostV1 {
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
            //use snafu::IntoError;
            //Err(Bad{msg:format!("API error")}.into_error(NoneError)).ctxb(SE!(AddPos))
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

pub async fn proxy_distribute_v1(req: Request<Body>) -> Result<Response<Body>, Error> {
    let (mut sink, body) = Body::channel();
    let uri = format!("http://sf-daqbuf-33:8371{}", req.uri().path());
    let res = Response::builder().status(StatusCode::OK).body(body)?;
    tokio::spawn(async move {
        let req = Request::builder().method(Method::GET).uri(uri).body(Body::empty())?;
        let res = Client::new().request(req).await?;
        if res.status() == StatusCode::OK {
            let (_heads, mut body) = res.into_parts();
            loop {
                use hyper::body::HttpBody;
                let chunk = body.data().await;
                if let Some(k) = chunk {
                    let k = k?;
                    sink.send_data(k).await?;
                } else {
                    break;
                }
            }
        }
        Ok::<_, Error>(())
    });
    Ok(res)
}
