pub mod api4;

use crate::api1::{channel_search_configs_v1, channel_search_list_v1, gather_json_2_v1};
use crate::err::Error;
use crate::gather::{gather_get_json_generic, SubRes};
use crate::pulsemap::MapPulseQuery;
use crate::{api_1_docs, api_4_docs, response, response_err, Cont, ReqCtx, PSI_DAQBUFFER_SERVICE_MARK};
use futures_util::{pin_mut, Stream};
use http::{Method, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use itertools::Itertools;
use netpod::log::*;
use netpod::query::{BinnedQuery, ChannelStateEventsQuery, PlainEventsQuery};
use netpod::{AppendToUrl, ChannelConfigQuery, FromUrl, HasBackend, HasTimeout, ProxyConfig};
use netpod::{ChannelSearchQuery, ChannelSearchResult, ChannelSearchSingleResult};
use netpod::{ACCEPT_ALL, APP_JSON};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};
use url::Url;

const DISTRI_PRE: &str = "/distri/";

pub async fn proxy(proxy_config: ProxyConfig) -> Result<(), Error> {
    use std::str::FromStr;
    let addr = SocketAddr::from_str(&format!("{}:{}", proxy_config.listen, proxy_config.port))?;
    let make_service = make_service_fn({
        move |_conn| {
            let proxy_config = proxy_config.clone();
            async move {
                Ok::<_, Error>(service_fn({
                    move |req| {
                        // TODO send to logstash
                        info!(
                            "REQUEST  {:?} - {:?} - {:?} - {:?}",
                            addr,
                            req.method(),
                            req.uri(),
                            req.headers()
                        );
                        let f = proxy_http_service(req, proxy_config.clone());
                        Cont { f: Box::pin(f) }
                    }
                }))
            }
        }
    });
    Server::bind(&addr).serve(make_service).await?;
    Ok(())
}

async fn proxy_http_service(req: Request<Body>, proxy_config: ProxyConfig) -> Result<Response<Body>, Error> {
    match proxy_http_service_try(req, &proxy_config).await {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("data_api_proxy sees error: {:?}", e);
            Err(e)
        }
    }
}

async fn proxy_http_service_try(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let ctx = ReqCtx::with_proxy(&req, proxy_config);
    let mut res = proxy_http_service_inner(req, &ctx, proxy_config).await?;
    let hm = res.headers_mut();
    hm.insert("Access-Control-Allow-Origin", "*".parse().unwrap());
    hm.insert("Access-Control-Allow-Headers", "*".parse().unwrap());
    for m in &ctx.marks {
        hm.append(PSI_DAQBUFFER_SERVICE_MARK, m.parse().unwrap());
    }
    hm.append(PSI_DAQBUFFER_SERVICE_MARK, ctx.mark.parse().unwrap());
    Ok(res)
}

async fn proxy_http_service_inner(
    req: Request<Body>,
    ctx: &ReqCtx,
    proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/1/channels" {
        Ok(channel_search_list_v1(req, proxy_config).await?)
    } else if path == "/api/1/channels/config" {
        Ok(channel_search_configs_v1(req, proxy_config).await?)
    } else if path.starts_with("/api/1/gather/") {
        Ok(gather_json_2_v1(req, "/api/1/gather/", proxy_config).await?)
    } else if path == "/api/4/private/version" {
        if req.method() == Method::GET {
            let ver_maj: u32 = std::env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap_or(0);
            let ver_min: u32 = std::env!("CARGO_PKG_VERSION_MINOR").parse().unwrap_or(0);
            let ver_pat: u32 = std::env!("CARGO_PKG_VERSION_PATCH").parse().unwrap_or(0);
            let ret = serde_json::json!({
                "daqbuf_version": {
                    "major": ver_maj,
                    "minor": ver_min,
                    "patch": ver_pat,
                },
            });
            Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&ret)?))?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if let Some(h) = api4::StatusNodesRecursive::handler(&req) {
        h.handle(req, ctx, &proxy_config).await
    } else if path == "/api/4/backends" {
        Ok(backends(req, proxy_config).await?)
    } else if path == "/api/4/search/channel" {
        Ok(api4::channel_search(req, proxy_config).await?)
    } else if path == "/api/4/events" {
        Ok(proxy_single_backend_query::<PlainEventsQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/status/connection/events" {
        Ok(proxy_single_backend_query::<ChannelStateEventsQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/status/channel/events" {
        Ok(proxy_single_backend_query::<ChannelStateEventsQuery>(req, ctx, proxy_config).await?)
    } else if path.starts_with("/api/4/map/pulse/") {
        Ok(proxy_single_backend_query::<MapPulseQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/binned" {
        Ok(proxy_single_backend_query::<BinnedQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/channel/config" {
        Ok(proxy_single_backend_query::<ChannelConfigQuery>(req, ctx, proxy_config).await?)
    } else if path.starts_with("/api/1/documentation/") {
        if req.method() == Method::GET {
            api_1_docs(path)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with("/api/4/documentation/") {
        if req.method() == Method::GET {
            api_4_docs(path)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with("/api/4/test/http/204") {
        Ok(response(StatusCode::NO_CONTENT).body(Body::from("No Content"))?)
    } else if path.starts_with("/api/4/test/http/400") {
        Ok(response(StatusCode::BAD_REQUEST).body(Body::from("Bad Request"))?)
    } else if path.starts_with("/api/4/test/http/405") {
        Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::from("Method Not Allowed"))?)
    } else if path.starts_with("/api/4/test/http/406") {
        Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::from("Not Acceptable"))?)
    } else if path.starts_with("/api/4/test/log/error") {
        error!("{path}");
        Ok(response(StatusCode::OK).body(Body::empty())?)
    } else if path.starts_with("/api/4/test/log/warn") {
        warn!("{path}");
        Ok(response(StatusCode::OK).body(Body::empty())?)
    } else if path.starts_with("/api/4/test/log/info") {
        info!("{path}");
        Ok(response(StatusCode::OK).body(Body::empty())?)
    } else if path.starts_with("/api/4/test/log/debug") {
        debug!("{path}");
        Ok(response(StatusCode::OK).body(Body::empty())?)
    } else if path.starts_with(DISTRI_PRE) {
        proxy_distribute_v2(req).await
    } else {
        use std::fmt::Write;
        let mut body = String::new();
        let out = &mut body;
        write!(out, "<pre>\n")?;
        write!(out, "METHOD {:?}<br>\n", req.method())?;
        write!(out, "URI    {:?}<br>\n", req.uri())?;
        write!(out, "HOST   {:?}<br>\n", req.uri().host())?;
        write!(out, "PORT   {:?}<br>\n", req.uri().port())?;
        write!(out, "PATH   {:?}<br>\n", req.uri().path())?;
        write!(out, "QUERY  {:?}<br>\n", req.uri().query())?;
        for (hn, hv) in req.headers() {
            write!(out, "HEADER {hn:?}: {hv:?}<br>\n")?;
        }
        write!(out, "</pre>\n")?;
        Ok(response(StatusCode::NOT_FOUND).body(Body::from(body))?)
    }
}

pub async fn proxy_distribute_v2(req: Request<Body>) -> Result<Response<Body>, Error> {
    let path = req.uri().path();
    if path
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || ['/', '.', '-', '_'].contains(&c))
        && !path.contains("..")
    {}
    if req.method() == Method::GET {
        let s = FileStream {
            file: File::open(format!("/opt/distri/{}", &path[DISTRI_PRE.len()..])).await?,
        };
        Ok(response(StatusCode::OK).body(Body::wrap_stream(s))?)
    } else {
        Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
    }
}

pub struct FileStream {
    file: File,
}

impl Stream for FileStream {
    type Item = Result<Vec<u8>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut buf = vec![0; 1024 * 8];
        let mut rb = ReadBuf::new(&mut buf);
        let f = &mut self.file;
        pin_mut!(f);
        match f.poll_read(cx, &mut rb) {
            Ready(k) => match k {
                Ok(_) => {
                    let n = rb.filled().len();
                    if n == 0 {
                        Ready(None)
                    } else {
                        buf.truncate(n);
                        Ready(Some(Ok(buf)))
                    }
                }
                Err(e) => Ready(Some(Err(e.into()))),
            },
            Pending => Pending,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BackendsResponse {
    backends: Vec<String>,
}

pub async fn backends(_req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let backends: Vec<_> = proxy_config.backends.iter().map(|k| k.name.to_string()).collect();
    let res = BackendsResponse { backends };
    let ret = response(StatusCode::OK).body(Body::from(serde_json::to_vec(&res)?))?;
    Ok(ret)
}

pub async fn channel_search(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get(http::header::ACCEPT) {
        Some(v) => {
            if v == APP_JSON {
                let url = Url::parse(&format!("dummy:{}", head.uri))?;
                let query = ChannelSearchQuery::from_url(&url)?;
                let mut methods = vec![];
                let mut bodies = vec![];
                let mut urls = proxy_config
                    .backends
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh.url)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(_e) => Err(Error::with_msg(format!("parse error for: {:?}", sh))),
                    })
                    .fold_ok(vec![], |mut a, x| {
                        a.push(x);
                        methods.push(http::Method::GET);
                        bodies.push(None);
                        a
                    })?;
                // TODO probably no longer needed?
                if let (Some(hosts), Some(backends)) = (None::<&Vec<String>>, None::<&Vec<String>>) {
                    #[derive(Serialize)]
                    struct QueryApi0 {
                        backends: Vec<String>,
                        regex: String,
                        #[serde(rename = "sourceRegex")]
                        source_regex: String,
                        ordering: String,
                        reload: bool,
                    }
                    hosts.iter().zip(backends.iter()).for_each(|(sh, back)| {
                        let url = Url::parse(&format!("{}/channels/config", sh)).unwrap();
                        urls.push(url);
                        let q = QueryApi0 {
                            backends: vec![back.into()],
                            ordering: "asc".into(),
                            reload: false,
                            regex: query.name_regex.clone(),
                            source_regex: query.source_regex.clone(),
                        };
                        let qs = serde_json::to_string(&q).unwrap();
                        methods.push(http::Method::POST);
                        bodies.push(Some(Body::from(qs)));
                    });
                }
                let tags = urls.iter().map(|k| k.to_string()).collect();
                let nt = |tag, res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        //info!("got a result {:?}", body);
                        let res: SubRes<ChannelSearchResult> =
                            match serde_json::from_slice::<ChannelSearchResult>(&body) {
                                Ok(val) => {
                                    let ret = SubRes {
                                        tag,
                                        status: StatusCode::OK,
                                        val,
                                    };
                                    ret
                                }
                                Err(_) => {
                                    #[derive(Deserialize)]
                                    struct ResItemApi0 {
                                        name: String,
                                        source: String,
                                        backend: String,
                                        #[serde(rename = "type")]
                                        ty: String,
                                    }
                                    #[derive(Deserialize)]
                                    struct ResContApi0 {
                                        #[allow(dead_code)]
                                        backend: String,
                                        channels: Vec<ResItemApi0>,
                                    }
                                    match serde_json::from_slice::<Vec<ResContApi0>>(&body) {
                                        Ok(k) => {
                                            let mut a = vec![];
                                            if let Some(g) = k.first() {
                                                for c in &g.channels {
                                                    let z = ChannelSearchSingleResult {
                                                        backend: c.backend.clone(),
                                                        description: String::new(),
                                                        name: c.name.clone(),
                                                        // TODO api 0 does not provide a series id
                                                        series: 0,
                                                        shape: vec![],
                                                        source: c.source.clone(),
                                                        ty: c.ty.clone(),
                                                        unit: String::new(),
                                                        is_api_0: Some(true),
                                                    };
                                                    a.push(z);
                                                }
                                            }
                                            let ret = ChannelSearchResult { channels: a };
                                            let ret = SubRes {
                                                tag,
                                                status: StatusCode::OK,
                                                val: ret,
                                            };
                                            ret
                                        }
                                        Err(_) => {
                                            error!("Channel search response parse failed");
                                            let ret = ChannelSearchResult { channels: vec![] };
                                            let ret = SubRes {
                                                tag,
                                                status: StatusCode::OK,
                                                val: ret,
                                            };
                                            ret
                                        }
                                    }
                                }
                            };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<(crate::gather::Tag, Result<SubRes<ChannelSearchResult>, Error>)>| {
                    let mut res = Vec::new();
                    for (_tag, j) in all {
                        match j {
                            Ok(j) => {
                                for k in j.val.channels {
                                    res.push(k);
                                }
                            }
                            Err(e) => {
                                warn!("{e}");
                            }
                        }
                    }
                    let res = ChannelSearchResult { channels: res };
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, APP_JSON)
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                // TODO gather_get_json_generic must for this case accept a Method for each Request.
                // Currently it is inferred via presence of the body.
                // On the other hand, I want to gather over rather homogeneous requests.
                // So: better enforce same method.
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

pub async fn proxy_single_backend_query<QT>(
    req: Request<Body>,
    _ctx: &ReqCtx,
    proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error>
where
    QT: FromUrl + AppendToUrl + HasBackend + HasTimeout,
{
    let (head, _body) = req.into_parts();
    info!("proxy_single_backend_query {}", head.uri);
    match head.headers.get(http::header::ACCEPT) {
        Some(v) => {
            if v == APP_JSON || v == ACCEPT_ALL {
                let url = Url::parse(&format!("dummy:{}", head.uri))?;
                let query = match QT::from_url(&url) {
                    Ok(k) => k,
                    Err(_) => {
                        let msg = format!("Malformed request or missing parameters");
                        return Ok(response_err(StatusCode::BAD_REQUEST, msg)?);
                    }
                };
                let sh = get_query_host_for_backend(&query.backend(), proxy_config)?;
                // TODO remove this special case
                // SPECIAL CASE:
                // Since the inner proxy is not yet handling map-pulse requests without backend,
                // we can not simply copy the original url here.
                // Instead, url needs to get parsed and formatted.
                // In general, the caller of this function should be able to provide a url, or maybe
                // better a closure so that the url can even depend on backend.
                let uri_path: String = if url.as_str().contains("/map/pulse/") {
                    match MapPulseQuery::from_url(&url) {
                        Ok(qu) => {
                            info!("qu {qu:?}");
                            format!("/api/4/map/pulse/{}/{}", qu.backend, qu.pulse)
                        }
                        Err(e) => {
                            error!("{e:?}");
                            String::from("/BAD")
                        }
                    }
                } else {
                    head.uri.path().into()
                };
                info!("uri_path {uri_path}");
                let urls = [sh]
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}{}", sh, uri_path)) {
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
                let nt = |tag: String, res: Response<Body>| {
                    let fut = async {
                        let (head, body) = res.into_parts();
                        if head.status == StatusCode::OK {
                            let body = hyper::body::to_bytes(body).await?;
                            match serde_json::from_slice::<JsonValue>(&body) {
                                Ok(val) => {
                                    let ret = SubRes {
                                        tag,
                                        status: head.status,
                                        val,
                                    };
                                    Ok(ret)
                                }
                                Err(e) => {
                                    warn!("can not parse response: {e:?}");
                                    Err(e.into())
                                }
                            }
                        } else {
                            let body = hyper::body::to_bytes(body).await?;
                            let b = String::from_utf8_lossy(&body);
                            let ret = SubRes {
                                tag,
                                status: head.status,
                                // TODO would like to pass arbitrary type of body in these cases:
                                val: serde_json::Value::String(format!("{}", b)),
                            };
                            Ok(ret)
                        }
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = Result<SubRes<serde_json::Value>, Error>> + Send>>
                };
                let ft = |mut all: Vec<(crate::gather::Tag, Result<SubRes<JsonValue>, Error>)>| {
                    if all.len() > 0 {
                        all.truncate(1);
                        let (_tag, z) = all.pop().unwrap();
                        match z {
                            Ok(z) => {
                                let res = z.val;
                                // TODO want to pass arbitrary body type:
                                let res = response(z.status)
                                    .header(http::header::CONTENT_TYPE, APP_JSON)
                                    .body(Body::from(serde_json::to_string(&res)?))?;
                                return Ok(res);
                            }
                            Err(e) => {
                                warn!("{e}");
                                let res = crate::bodystream::ToPublicResponse::to_public_response(&e);
                                return Ok(res);
                            }
                        }
                    } else {
                        return Err(Error::with_msg("no response from upstream"));
                    }
                };
                let bodies = (0..urls.len()).into_iter().map(|_| None).collect();
                let ret =
                    gather_get_json_generic(http::Method::GET, urls, bodies, tags, nt, ft, query.timeout()).await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

fn get_query_host_for_backend(backend: &str, proxy_config: &ProxyConfig) -> Result<String, Error> {
    for back in &proxy_config.backends {
        if back.name == backend {
            return Ok(back.url.clone());
        }
    }
    return Err(Error::with_msg(format!("host not found for backend {:?}", backend)));
}
