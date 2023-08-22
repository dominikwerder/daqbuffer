pub mod api1;
pub mod api4;
pub mod bodystream;
pub mod channel_status;
pub mod channelconfig;
pub mod download;
pub mod err;
pub mod gather;
pub mod prometheus;
pub mod proxy;
pub mod pulsemap;
pub mod settings;

use self::bodystream::ToPublicResponse;
use crate::bodystream::response;
use crate::err::Error;
use crate::gather::gather_get_json;
use ::err::thiserror;
use ::err::ThisError;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::StreamExt;
use http::Method;
use http::StatusCode;
use hyper::server::conn::AddrStream;
use hyper::server::Server;
use hyper::service::make_service_fn;
use hyper::service::service_fn;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use net::SocketAddr;
use netpod::log::*;
use netpod::query::prebinned::PreBinnedQuery;
use netpod::NodeConfigCached;
use netpod::ProxyConfig;
use netpod::ServiceVersion;
use netpod::APP_JSON;
use netpod::APP_JSON_LINES;
use panic::AssertUnwindSafe;
use panic::UnwindSafe;
use pin::Pin;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::net;
use std::panic;
use std::pin;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Once;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
use std::task;
use std::time::SystemTime;
use task::Context;
use task::Poll;

pub const PSI_DAQBUFFER_SERVICE_MARK: &'static str = "PSI-Daqbuffer-Service-Mark";
pub const PSI_DAQBUFFER_SEEN_URL: &'static str = "PSI-Daqbuffer-Seen-Url";

#[derive(Debug, ThisError, Serialize, Deserialize)]
pub enum RetrievalError {
    Error(#[from] ::err::Error),
    Error2(#[from] crate::err::Error),
    TextError(String),
    #[serde(skip)]
    Hyper(#[from] hyper::Error),
    #[serde(skip)]
    Http(#[from] http::Error),
    #[serde(skip)]
    Serde(#[from] serde_json::Error),
    #[serde(skip)]
    Fmt(#[from] std::fmt::Error),
    #[serde(skip)]
    Url(#[from] url::ParseError),
}

trait IntoBoxedError: std::error::Error {}
impl IntoBoxedError for net::AddrParseError {}
impl IntoBoxedError for tokio::task::JoinError {}
impl IntoBoxedError for api4::databuffer_tools::FindActiveError {}
impl IntoBoxedError for std::string::FromUtf8Error {}

impl<E> From<E> for RetrievalError
where
    E: ToString + IntoBoxedError,
{
    fn from(value: E) -> Self {
        Self::TextError(value.to_string())
    }
}

impl ::err::ToErr for RetrievalError {
    fn to_err(self) -> ::err::Error {
        ::err::Error::with_msg_no_trace(self.to_string())
    }
}

pub fn accepts_json(hm: &http::HeaderMap) -> bool {
    match hm.get(http::header::ACCEPT) {
        Some(x) => match x.to_str() {
            Ok(x) => x.contains(netpod::APP_JSON) || x.contains(netpod::ACCEPT_ALL),
            Err(_) => false,
        },
        None => false,
    }
}

pub fn accepts_octets(hm: &http::HeaderMap) -> bool {
    match hm.get(http::header::ACCEPT) {
        Some(x) => match x.to_str() {
            Ok(x) => x.contains(netpod::APP_OCTET),
            Err(_) => false,
        },
        None => false,
    }
}

pub async fn host(node_config: NodeConfigCached, service_version: ServiceVersion) -> Result<(), RetrievalError> {
    static STATUS_BOARD_INIT: Once = Once::new();
    STATUS_BOARD_INIT.call_once(|| {
        let b = StatusBoard::new();
        let a = RwLock::new(b);
        let x = Box::new(a);
        STATUS_BOARD.store(Box::into_raw(x), Ordering::SeqCst);
    });
    if let Some(bind) = node_config.node.prometheus_api_bind {
        tokio::spawn(prometheus::host(bind));
    }
    // let rawjh = taskrun::spawn(nodenet::conn::events_service(node_config.clone()));
    use std::str::FromStr;
    let addr = SocketAddr::from_str(&format!("{}:{}", node_config.node.listen(), node_config.node.port))?;
    let make_service = make_service_fn({
        move |conn: &AddrStream| {
            debug!("new connection from {:?}", conn.remote_addr());
            let node_config = node_config.clone();
            let addr = conn.remote_addr();
            let service_version = service_version.clone();
            async move {
                let ret = service_fn(move |req| {
                    // TODO send to logstash
                    info!(
                        "http-request  {:?} - {:?} - {:?} - {:?}",
                        addr,
                        req.method(),
                        req.uri(),
                        req.headers()
                    );
                    let f = http_service(req, node_config.clone(), service_version.clone());
                    Cont { f: Box::pin(f) }
                });
                Ok::<_, Error>(ret)
            }
        }
    });
    Server::bind(&addr)
        .serve(make_service)
        .await
        .map(|e| RetrievalError::TextError(format!("{e:?}")))?;
    // rawjh.await??;
    Ok(())
}

async fn http_service(
    req: Request<Body>,
    node_config: NodeConfigCached,
    service_version: ServiceVersion,
) -> Result<Response<Body>, Error> {
    match http_service_try(req, &node_config, &service_version).await {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("daqbuffer node http_service sees error: {}", e);
            Err(e)
        }
    }
}

// TODO move this and related stuff to separate module
struct Cont<F> {
    f: Pin<Box<F>>,
}

impl<F, I> Future for Cont<F>
where
    F: Future<Output = Result<I, Error>>,
{
    type Output = <F as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let h = std::panic::catch_unwind(AssertUnwindSafe(|| self.f.poll_unpin(cx)));
        match h {
            Ok(k) => k,
            Err(e) => {
                error!("Cont<F>  catch_unwind  {e:?}");
                match e.downcast_ref::<Error>() {
                    Some(e) => {
                        error!("Cont<F>  catch_unwind  is Error: {e:?}");
                    }
                    None => {}
                }
                Poll::Ready(Err(Error::with_msg_no_trace(format!("{e:?}"))))
            }
        }
    }
}

impl<F> UnwindSafe for Cont<F> {}

pub struct ReqCtx {
    pub marks: Vec<String>,
    pub mark: String,
}

impl ReqCtx {
    fn with_node<T>(req: &Request<T>, nc: &NodeConfigCached) -> Self {
        let mut marks = Vec::new();
        for (n, v) in req.headers().iter() {
            if n == PSI_DAQBUFFER_SERVICE_MARK {
                marks.push(String::from_utf8_lossy(v.as_bytes()).to_string());
            }
        }
        Self {
            marks,
            mark: format!("{}:{}", nc.node_config.name, nc.node.port),
        }
    }
}

impl ReqCtx {
    fn with_proxy<T>(req: &Request<T>, proxy: &ProxyConfig) -> Self {
        let mut marks = Vec::new();
        for (n, v) in req.headers().iter() {
            if n == PSI_DAQBUFFER_SERVICE_MARK {
                marks.push(String::from_utf8_lossy(v.as_bytes()).to_string());
            }
        }
        Self {
            marks,
            mark: format!("{}:{}", proxy.name, proxy.port),
        }
    }
}

// TODO remove because I want error bodies to be json.
pub fn response_err<T>(status: StatusCode, msg: T) -> Result<Response<Body>, RetrievalError>
where
    T: AsRef<str>,
{
    let msg = format!(
        concat!(
            "Error:\n{}\n",
            "\nDocumentation pages API 1 and 4:",
            "\nhttps://data-api.psi.ch/api/1/documentation/",
            "\nhttps://data-api.psi.ch/api/4/documentation/",
        ),
        msg.as_ref()
    );
    let ret = response(status).body(Body::from(msg))?;
    Ok(ret)
}

macro_rules! static_http {
    ($path:expr, $tgt:expr, $tgtex:expr, $ctype:expr) => {
        if $path == concat!("/api/4/documentation/", $tgt) {
            let c = include_bytes!(concat!("../static/documentation/", $tgtex));
            let ret = response(StatusCode::OK)
                .header("content-type", $ctype)
                .body(Body::from(&c[..]))?;
            return Ok(ret);
        }
    };
    ($path:expr, $tgt:expr, $ctype:expr) => {
        if $path == concat!("/api/4/documentation/", $tgt) {
            let c = include_bytes!(concat!("../static/documentation/", $tgt));
            let ret = response(StatusCode::OK)
                .header("content-type", $ctype)
                .body(Body::from(&c[..]))?;
            return Ok(ret);
        }
    };
}

macro_rules! static_http_api1 {
    ($path:expr, $tgt:expr, $tgtex:expr, $ctype:expr) => {
        if $path == concat!("/api/1/documentation/", $tgt) {
            let c = include_bytes!(concat!("../static/documentation/", $tgtex));
            let ret = response(StatusCode::OK)
                .header("content-type", $ctype)
                .body(Body::from(&c[..]))?;
            return Ok(ret);
        }
    };
    ($path:expr, $tgt:expr, $ctype:expr) => {
        if $path == concat!("/api/1/documentation/", $tgt) {
            let c = include_bytes!(concat!("../static/documentation/", $tgt));
            let ret = response(StatusCode::OK)
                .header("content-type", $ctype)
                .body(Body::from(&c[..]))?;
            return Ok(ret);
        }
    };
}

async fn http_service_try(
    req: Request<Body>,
    node_config: &NodeConfigCached,
    service_version: &ServiceVersion,
) -> Result<Response<Body>, Error> {
    use http::HeaderValue;
    let mut urlmarks = Vec::new();
    urlmarks.push(format!("{}:{}", req.method(), req.uri()));
    for (k, v) in req.headers() {
        if k == PSI_DAQBUFFER_SEEN_URL {
            let s = String::from_utf8_lossy(v.as_bytes());
            urlmarks.push(s.into());
        }
    }
    let ctx = ReqCtx::with_node(&req, node_config);
    let mut res = http_service_inner(req, &ctx, node_config, service_version).await?;
    let hm = res.headers_mut();
    hm.append("Access-Control-Allow-Origin", "*".parse().unwrap());
    hm.append("Access-Control-Allow-Headers", "*".parse().unwrap());
    for m in &ctx.marks {
        hm.append(PSI_DAQBUFFER_SERVICE_MARK, m.parse().unwrap());
    }
    hm.append(PSI_DAQBUFFER_SERVICE_MARK, ctx.mark.parse().unwrap());
    for s in urlmarks {
        let v = HeaderValue::from_str(&s).unwrap_or_else(|_| HeaderValue::from_static("invalid"));
        hm.append(PSI_DAQBUFFER_SEEN_URL, v);
    }
    Ok(res)
}

async fn http_service_inner(
    req: Request<Body>,
    ctx: &ReqCtx,
    node_config: &NodeConfigCached,
    service_version: &ServiceVersion,
) -> Result<Response<Body>, RetrievalError> {
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/4/private/version" {
        if req.method() == Method::GET {
            let ret = serde_json::json!({
                "daqbuf_version": {
                    "major": service_version.major,
                    "minor": service_version.minor,
                    "patch": service_version.patch,
                },
            });
            Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&ret)?))?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with("/api/4/private/logtest/") {
        if req.method() == Method::GET {
            if path.ends_with("/trace") {
                trace!("test trace log output");
            } else if path.ends_with("/debug") {
                debug!("test debug log output");
            } else if path.ends_with("/info") {
                info!("test info log output");
            } else if path.ends_with("/warn") {
                warn!("test warn log output");
            } else if path.ends_with("/error") {
                error!("test error log output");
            } else {
                error!("test unknown log output");
            }
            Ok(response(StatusCode::OK).body(Body::empty())?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if let Some(h) = api4::eventdata::EventDataHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config, service_version)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?)
    } else if let Some(h) = api4::status::StatusNodesRecursive::handler(&req) {
        Ok(h.handle(req, ctx, &node_config, service_version).await?)
    } else if let Some(h) = StatusBoardAllHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = api4::databuffer_tools::FindActiveHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api4::search::ChannelSearchHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api4::binned::BinnedHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ChannelConfigQuorumHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ChannelConfigsHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ChannelConfigHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ScyllaChannelsWithType::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::IocForChannel::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ScyllaChannelsActive::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ScyllaSeriesTsMsp::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::AmbigiousChannelNames::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api4::events::EventsHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channel_status::ConnectionStatusEvents::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if let Some(h) = channel_status::ChannelStatusEvents::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if path == "/api/4/prebinned" {
        if req.method() == Method::GET {
            Ok(prebinned(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/random/channel" {
        if req.method() == Method::GET {
            Ok(random_channel(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with("/api/4/gather/") {
        if req.method() == Method::GET {
            Ok(gather_get_json(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/clear_cache" {
        if req.method() == Method::GET {
            Ok(clear_cache_all(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/update_db_with_channel_names" {
        if req.method() == Method::GET {
            Ok(update_db_with_channel_names(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/update_db_with_all_channel_configs" {
        if req.method() == Method::GET {
            Ok(update_db_with_all_channel_configs(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/update_search_cache" {
        if req.method() == Method::GET {
            Ok(update_search_cache(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if let Some(h) = download::DownloadHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = settings::SettingsThreadsMaxHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api1::Api1EventsBinaryHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseScyllaHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::IndexFullHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::MarkClosedHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseLocalHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseHistoHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::Api4MapPulse2HttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::Api4MapPulseHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api1::RequestStatusHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
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

pub fn api_4_docs(path: &str) -> Result<Response<Body>, RetrievalError> {
    static_http!(path, "", "api4.html", "text/html");
    static_http!(path, "style.css", "text/css");
    static_http!(path, "script.js", "text/javascript");
    static_http!(path, "status-main.html", "text/html");
    Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
}

pub fn api_1_docs(path: &str) -> Result<Response<Body>, RetrievalError> {
    static_http_api1!(path, "", "api1.html", "text/html");
    static_http_api1!(path, "style.css", "text/css");
    static_http_api1!(path, "script.js", "text/javascript");
    Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
}

pub struct StatusBoardAllHandler {}

impl StatusBoardAllHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/status/board/all" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        _req: Request<Body>,
        _node_config: &NodeConfigCached,
    ) -> Result<Response<Body>, RetrievalError> {
        use std::ops::Deref;
        let sb = status_board().unwrap();
        let buf = serde_json::to_vec(sb.deref()).unwrap();
        let res = response(StatusCode::OK).body(Body::from(buf))?;
        Ok(res)
    }
}

async fn prebinned(
    req: Request<Body>,
    ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    match prebinned_inner(req, ctx, node_config).await {
        Ok(ret) => Ok(ret),
        Err(e) => {
            error!("fn prebinned: {e:?}");
            Ok(response(StatusCode::BAD_REQUEST).body(Body::from(format!("[prebinned-error]")))?)
        }
    }
}

async fn prebinned_inner(
    req: Request<Body>,
    _ctx: &ReqCtx,
    _node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    let (head, _body) = req.into_parts();
    let url: url::Url = format!("dummy://{}", head.uri).parse()?;
    let query = PreBinnedQuery::from_url(&url)?;
    let span1 = span!(Level::INFO, "httpret::prebinned", desc = &query.patch().span_desc());
    span1.in_scope(|| {
        debug!("begin");
    });
    error!("TODO httpret prebinned_inner");
    //let fut = disk::binned::prebinned::pre_binned_bytes_for_http(node_config, &query).instrument(span1);
    todo!()
}

async fn random_channel(
    req: Request<Body>,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    let (_head, _body) = req.into_parts();
    let ret = dbconn::random_channel(node_config).await?;
    let ret = response(StatusCode::OK).body(Body::from(ret))?;
    Ok(ret)
}

async fn clear_cache_all(
    req: Request<Body>,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    let (head, _body) = req.into_parts();
    let dry = match head.uri.query() {
        Some(q) => q.contains("dry"),
        None => false,
    };
    let res = disk::cache::clear_cache_all(node_config, dry).await?;
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON)
        .body(Body::from(serde_json::to_string(&res)?))?;
    Ok(ret)
}

async fn update_db_with_channel_names(
    req: Request<Body>,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    info!("httpret::update_db_with_channel_names");
    let (head, _body) = req.into_parts();
    let _dry = match head.uri.query() {
        Some(q) => q.contains("dry"),
        None => false,
    };
    let res =
        dbconn::scan::update_db_with_channel_names(node_config.clone(), &node_config.node_config.cluster.database)
            .await;
    match res {
        Ok(res) => {
            let ret = response(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
                .body(Body::wrap_stream(res.map(|k| match serde_json::to_string(&k) {
                    Ok(mut item) => {
                        item.push('\n');
                        Ok(item)
                    }
                    Err(e) => Err(e),
                })))?;
            Ok(ret)
        }
        Err(e) => {
            let p = serde_json::to_string(&e)?;
            let res = response(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
                .body(Body::from(p))?;
            Ok(res)
        }
    }
}

#[allow(unused)]
async fn update_db_with_channel_names_3(
    req: Request<Body>,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    let (head, _body) = req.into_parts();
    let _dry = match head.uri.query() {
        Some(q) => q.contains("dry"),
        None => false,
    };
    let res = dbconn::scan::update_db_with_channel_names_3(node_config);
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
        .body(Body::wrap_stream(res.map(|k| match serde_json::to_string(&k) {
            Ok(mut item) => {
                item.push('\n');
                Ok(item)
            }
            Err(e) => Err(e),
        })))?;
    Ok(ret)
}

async fn update_db_with_all_channel_configs(
    req: Request<Body>,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    let (head, _body) = req.into_parts();
    let _dry = match head.uri.query() {
        Some(q) => q.contains("dry"),
        None => false,
    };
    let res = dbconn::scan::update_db_with_all_channel_configs(node_config.clone()).await?;
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
        .body(Body::wrap_stream(res.map(|k| match serde_json::to_string(&k) {
            Ok(mut item) => {
                item.push('\n');
                Ok(item)
            }
            Err(e) => Err(e),
        })))?;
    Ok(ret)
}

async fn update_search_cache(
    req: Request<Body>,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, RetrievalError> {
    let (head, _body) = req.into_parts();
    let _dry = match head.uri.query() {
        Some(q) => q.contains("dry"),
        None => false,
    };
    let res = dbconn::scan::update_search_cache(node_config).await?;
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON)
        .body(Body::from(serde_json::to_string(&res)?))?;
    Ok(ret)
}

#[derive(Debug, Serialize)]
pub struct StatusBoardEntry {
    #[allow(unused)]
    #[serde(serialize_with = "instant_serde::ser")]
    ts_created: SystemTime,
    #[serde(serialize_with = "instant_serde::ser")]
    ts_updated: SystemTime,
    // #[serde(skip_serializing_if = "is_false")]
    done: bool,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<::err::Error>,
    // TODO make this a better Stats container and remove pub access.
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    error_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    warn_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    channel_not_found: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    subreq_fail: usize,
}

mod instant_serde {
    use super::*;
    use netpod::DATETIME_FMT_3MS;
    use serde::Serializer;

    pub fn ser<S: Serializer>(x: &SystemTime, ser: S) -> Result<S::Ok, S::Error> {
        use chrono::LocalResult;
        let dur = x.duration_since(std::time::UNIX_EPOCH).unwrap();
        let res = chrono::TimeZone::timestamp_opt(&chrono::Utc, dur.as_secs() as i64, dur.subsec_nanos());
        match res {
            LocalResult::None => Err(serde::ser::Error::custom(format!("Bad local instant conversion"))),
            LocalResult::Single(dt) => {
                let s = dt.format(DATETIME_FMT_3MS).to_string();
                ser.serialize_str(&s)
            }
            LocalResult::Ambiguous(dt, _dt2) => {
                let s = dt.format(DATETIME_FMT_3MS).to_string();
                ser.serialize_str(&s)
            }
        }
    }
}

impl StatusBoardEntry {
    pub fn new() -> Self {
        Self {
            ts_created: SystemTime::now(),
            ts_updated: SystemTime::now(),
            done: false,
            errors: Vec::new(),
            error_count: 0,
            warn_count: 0,
            channel_not_found: 0,
            subreq_fail: 0,
        }
    }

    pub fn warn_inc(&mut self) {
        self.warn_count += 1;
    }

    pub fn channel_not_found_inc(&mut self) {
        self.channel_not_found += 1;
    }
}

#[derive(Debug, Serialize)]
pub struct StatusBoardEntryUser {
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    error_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    warn_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    channel_not_found: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<::err::PublicError>,
}

impl From<&StatusBoardEntry> for StatusBoardEntryUser {
    fn from(e: &StatusBoardEntry) -> Self {
        Self {
            error_count: e.error_count,
            warn_count: e.warn_count,
            channel_not_found: e.channel_not_found,
            errors: e.errors.iter().map(|e| e.to_public_error()).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatusBoard {
    entries: BTreeMap<String, StatusBoardEntry>,
}

impl StatusBoard {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn new_status_id(&mut self) -> String {
        use std::fs::File;
        use std::io::Read;
        self.clean();
        let mut f = File::open("/dev/urandom").unwrap();
        let mut buf = [0; 4];
        f.read_exact(&mut buf).unwrap();
        let n = u32::from_le_bytes(buf);
        let s = format!("{:08x}", n);
        debug!("new_status_id {s}");
        self.entries.insert(s.clone(), StatusBoardEntry::new());
        s
    }

    pub fn clean(&mut self) {
        if self.entries.len() > 15000 {
            let mut tss: Vec<_> = self.entries.values().map(|e| e.ts_updated).collect();
            tss.sort_unstable();
            let tss = tss;
            let tsm = tss[tss.len() / 3];
            let a = std::mem::replace(&mut self.entries, BTreeMap::new());
            self.entries = a.into_iter().filter(|(_k, v)| v.ts_updated >= tsm).collect();
        }
    }

    pub fn get_entry(&mut self, status_id: &str) -> Option<&mut StatusBoardEntry> {
        self.entries.get_mut(status_id)
    }

    pub fn mark_alive(&mut self, status_id: &str) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn mark_done(&mut self, status_id: &str) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
                e.done = true;
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn add_error(&mut self, status_id: &str, err: ::err::Error) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
                if e.errors.len() < 100 {
                    e.errors.push(err);
                    e.error_count += 1;
                }
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn status_as_json(&self, status_id: &str) -> StatusBoardEntryUser {
        match self.entries.get(status_id) {
            Some(e) => e.into(),
            None => {
                error!("can not find status id {}", status_id);
                let _e = ::err::Error::with_public_msg_no_trace(format!("Request status ID unknown {status_id}"));
                StatusBoardEntryUser {
                    error_count: 1,
                    warn_count: 0,
                    channel_not_found: 0,
                    errors: vec![::err::Error::with_public_msg_no_trace("request-id not found").into()],
                }
            }
        }
    }
}

static STATUS_BOARD: AtomicPtr<RwLock<StatusBoard>> = AtomicPtr::new(std::ptr::null_mut());

pub fn status_board() -> Result<RwLockWriteGuard<'static, StatusBoard>, RetrievalError> {
    let x = unsafe { &*STATUS_BOARD.load(Ordering::SeqCst) }.write();
    match x {
        Ok(x) => Ok(x),
        Err(e) => Err(RetrievalError::TextError(format!("{e}"))),
    }
}
