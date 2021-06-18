use crate::response;
use err::Error;
use http::{Method, StatusCode};
use hyper::{Body, Client, Request, Response};
use netpod::log::*;
use netpod::{ProxyBackend, ProxyConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::timeout_at;

fn get_live_hosts() -> &'static [(&'static str, u16)] {
    // TODO take from config.
    err::todo();
    &[]
}

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

pub async fn channels_list_v1(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let reqbody = req.into_body();
    let bodyslice = hyper::body::to_bytes(reqbody).await?;
    let query: ChannelSearchQueryV1 = serde_json::from_slice(&bodyslice)?;
    let subq_maker = |backend: &str| -> JsonValue {
        serde_json::to_value(ChannelSearchQueryV1 {
            regex: query.regex.clone(),
            source_regex: query.source_regex.clone(),
            description_regex: query.description_regex.clone(),
            backends: vec![backend.into()],
            ordering: query.ordering.clone(),
        })
        .unwrap()
    };
    let back2: Vec<_> = query.backends.iter().map(|x| x.as_str()).collect();
    let spawned = subreq(&back2[..], "channels", &subq_maker, proxy_config)?;
    let mut res = vec![];
    for (backend, s) in spawned {
        res.push((backend, s.await));
    }
    let res2 = ChannelSearchResultV1(extr(res));
    let body = serde_json::to_string(&res2.0)?;
    let res = response(StatusCode::OK).body(body.into())?;
    Ok(res)
}

type TT0 = (ProxyBackend, http::response::Parts, hyper::body::Bytes);
type TT1 = Result<TT0, Error>;
type TT2 = tokio::task::JoinHandle<TT1>;
type TT3 = Result<TT1, tokio::task::JoinError>;
type TT4 = Result<TT3, tokio::time::error::Elapsed>;
type TT7 = Pin<Box<dyn Future<Output = TT4> + Send>>;
type TT8 = (String, TT7);

fn subreq(
    backends_req: &[&str],
    endp: &str,
    subq_maker: &dyn Fn(&str) -> JsonValue,
    proxy_config: &ProxyConfig,
) -> Result<Vec<TT8>, Error> {
    let backends = proxy_config.backends.clone();
    let mut spawned = vec![];
    for back in &backends {
        if backends_req.contains(&back.name.as_str()) {
            let back = back.clone();
            let q = subq_maker(&back.name);
            let endp = match back.name.as_str() {
                "timeout" => "channels_timeout",
                "error500" => "channels_error500",
                _ => endp,
            };
            let uri = format!("http://{}:{}{}/{}", back.host, back.port, "/api/1", endp);
            let req = Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&q)?))?;
            let jh: TT2 = tokio::spawn({
                let back = back.clone();
                async move {
                    let res = Client::new().request(req).await?;
                    let (pre, body) = res.into_parts();
                    //info!("Answer from {}  status {}", back.1, pre.status);
                    let body_all = hyper::body::to_bytes(body).await?;
                    //info!("Got {} bytes from {}", body_all.len(), back.1);
                    Ok::<_, Error>((back, pre, body_all))
                }
            });
            let jh = tokio::time::timeout(std::time::Duration::from_millis(5000), jh);
            let bx: TT7 = Box::pin(jh);
            spawned.push((back.name.clone(), bx));
        }
    }
    Ok(spawned)
}

//fn extr<'a, T: BackendAware + FromErrorCode + Deserialize<'a>>(results: Vec<(&str, TT4)>) -> Vec<T> {
fn extr<T: BackendAware + FromErrorCode + for<'a> Deserialize<'a>>(results: Vec<(String, TT4)>) -> Vec<T> {
    let mut ret = vec![];
    for (backend, r) in results {
        if let Ok(r20) = r {
            if let Ok(r30) = r20 {
                if let Ok(r2) = r30 {
                    if r2.1.status == 200 {
                        let inp_res: Result<Vec<T>, _> = serde_json::from_slice(&r2.2);
                        if let Ok(inp) = inp_res {
                            if inp.len() > 1 {
                                error!("more than one result item from {:?}", r2.0);
                            } else {
                                for inp2 in inp {
                                    if inp2.backend() == r2.0.name {
                                        ret.push(inp2);
                                    }
                                }
                            }
                        } else {
                            error!("malformed answer from {:?}", r2.0);
                            ret.push(T::from_error_code(backend.as_str(), ErrorCode::Error));
                        }
                    } else {
                        error!("bad answer from {:?}", r2.0);
                        ret.push(T::from_error_code(backend.as_str(), ErrorCode::Error));
                    }
                } else {
                    error!("bad answer from {:?}", r30);
                    ret.push(T::from_error_code(backend.as_str(), ErrorCode::Error));
                }
            } else {
                error!("subrequest join handle error {:?}", r20);
                ret.push(T::from_error_code(backend.as_str(), ErrorCode::Error));
            }
        } else {
            error!("subrequest timeout {:?}", r);
            ret.push(T::from_error_code(backend.as_str(), ErrorCode::Timeout));
        }
    }
    ret
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

pub async fn channels_config_v1(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let reqbody = req.into_body();
    let bodyslice = hyper::body::to_bytes(reqbody).await?;
    let query: ChannelConfigsQueryV1 = serde_json::from_slice(&bodyslice)?;
    let subq_maker = |backend: &str| -> JsonValue {
        serde_json::to_value(ChannelConfigsQueryV1 {
            regex: query.regex.clone(),
            source_regex: query.source_regex.clone(),
            description_regex: query.description_regex.clone(),
            backends: vec![backend.into()],
            ordering: query.ordering.clone(),
        })
        .unwrap()
    };
    let back2: Vec<_> = query.backends.iter().map(|x| x.as_str()).collect();
    let spawned = subreq(&back2[..], "channels/config", &subq_maker, proxy_config)?;
    let mut res = vec![];
    for (backend, s) in spawned {
        res.push((backend, s.await));
    }
    let res2 = ChannelConfigsResponseV1(extr(res));
    let body = serde_json::to_string(&res2.0)?;
    let res = response(StatusCode::OK).body(body.into())?;
    Ok(res)
}

pub async fn gather_json_v1(req_m: Request<Body>, path: &str) -> Result<Response<Body>, Error> {
    // TODO can this be removed?
    err::todo();
    let mut spawned = vec![];
    let (req_h, _) = req_m.into_parts();
    for host in get_live_hosts() {
        for inst in &["00", "01", "02"] {
            let req_hh = req_h.headers.clone();
            let host_filter = if req_hh.contains_key("host_filter") {
                Some(req_hh.get("host_filter").unwrap().to_str().unwrap())
            } else {
                None
            };
            let path = path.to_string();
            let task = if host_filter.is_none() || host_filter.as_ref().unwrap() == &host.0 {
                let task = (
                    host.clone(),
                    inst.to_string(),
                    tokio::spawn(async move {
                        let uri = format!("http://{}:{}{}", host.0, host.1, path);
                        let req = Request::builder().method(Method::GET).uri(uri);
                        let req = if false && req_hh.contains_key("retrieval_instance") {
                            req.header("retrieval_instance", req_hh.get("retrieval_instance").unwrap())
                        } else {
                            req
                        };
                        let req = req.header("retrieval_instance", *inst);
                        //.header("content-type", "application/json")
                        //.body(Body::from(serde_json::to_string(&q)?))?;
                        let req = req.body(Body::empty())?;
                        let deadline = tokio::time::Instant::now() + Duration::from_millis(1000);
                        let fut = async {
                            let res = Client::new().request(req).await?;
                            let (pre, body) = res.into_parts();
                            if pre.status != StatusCode::OK {
                                Err(Error::with_msg(format!("request failed, got {}", pre.status)))
                            } else {
                                // aggregate returns a hyper Buf which is not Read
                                let body_all = hyper::body::to_bytes(body).await?;
                                let val = match serde_json::from_slice(&body_all) {
                                    Ok(k) => k,
                                    Err(_e) => JsonValue::String(String::from_utf8(body_all.to_vec())?),
                                };
                                Ok(val)
                            }
                        };
                        let ret = timeout_at(deadline, fut).await??;
                        Ok::<_, Error>(ret)
                    }),
                );
                Some(task)
            } else {
                None
            };
            if let Some(task) = task {
                spawned.push(task);
            }
        }
    }
    use serde_json::Map;
    let mut m = Map::new();
    for h in spawned {
        let res = match h.2.await {
            Ok(k) => match k {
                Ok(k) => k,
                Err(_e) => JsonValue::String(format!("ERROR")),
            },
            Err(_e) => JsonValue::String(format!("ERROR")),
        };
        m.insert(format!("{}:{}-{}", h.0 .0, h.0 .1, h.1), res);
    }
    let res = response(200)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&m)?.into())?;
    Ok(res)
}

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
        let req = req.header(http::header::ACCEPT, "application/json");
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
        .header(http::header::CONTENT_TYPE, "application/json")
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
