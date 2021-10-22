use crate::gather::gather_get_json;
use crate::pulsemap::UpdateTask;
use bytes::Bytes;
use disk::binned::query::{BinnedQuery, PreBinnedQuery};
use disk::events::{PlainEventsBinaryQuery, PlainEventsJsonQuery};
use err::Error;
use future::Future;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use http::{HeaderMap, Method, StatusCode};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{server::Server, Body, Request, Response};
use net::SocketAddr;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{
    channel_from_pairs, get_url_query_pairs, AggKind, ChannelConfigQuery, FromUrl, NodeConfigCached, APP_JSON,
    APP_JSON_LINES, APP_OCTET,
};
use nodenet::conn::events_service;
use panic::{AssertUnwindSafe, UnwindSafe};
use pin::Pin;
use serde::{Deserialize, Serialize};
use std::{future, net, panic, pin, task};
use task::{Context, Poll};
use tracing::field::Empty;
use tracing::Instrument;
use url::Url;

pub mod api1;
pub mod channelarchiver;
pub mod gather;
pub mod proxy;
pub mod pulsemap;
pub mod search;

fn proxy_mark() -> &'static str {
    "7c5e408a"
}

pub async fn host(node_config: NodeConfigCached) -> Result<(), Error> {
    let _update_task = if node_config.node_config.cluster.run_map_pulse_task {
        Some(UpdateTask::new(node_config.clone()))
    } else {
        None
    };
    let rawjh = taskrun::spawn(events_service(node_config.clone()));
    use std::str::FromStr;
    let addr = SocketAddr::from_str(&format!("{}:{}", node_config.node.listen, node_config.node.port))?;
    let make_service = make_service_fn({
        move |conn: &AddrStream| {
            info!("new connection from {:?}", conn.remote_addr());
            let node_config = node_config.clone();
            async move {
                Ok::<_, Error>(service_fn({
                    move |req| {
                        let f = http_service(req, node_config.clone());
                        Cont { f: Box::pin(f) }
                    }
                }))
            }
        }
    });
    Server::bind(&addr).serve(make_service).await?;
    rawjh.await??;
    Ok(())
}

async fn http_service(req: Request<Body>, node_config: NodeConfigCached) -> Result<Response<Body>, Error> {
    match http_service_try(req, &node_config).await {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("daqbuffer node http_service sees error: {:?}", e);
            Err(e)
        }
    }
}

struct Cont<F> {
    f: Pin<Box<F>>,
}

impl<F, I> Future for Cont<F>
where
    F: Future<Output = Result<I, Error>>,
{
    type Output = <F as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let h = std::panic::catch_unwind(AssertUnwindSafe(|| self.f.poll_unpin(cx)));
        match h {
            Ok(k) => k,
            Err(e) => {
                error!("Cont<F>  catch_unwind  {:?}", e);
                match e.downcast_ref::<Error>() {
                    Some(e) => {
                        error!("Cont<F>  catch_unwind  is Error: {:?}", e);
                    }
                    None => {}
                }
                Poll::Ready(Err(Error::from(format!("{:?}", e))))
            }
        }
    }
}

impl<F> UnwindSafe for Cont<F> {}

pub fn response_err<T>(status: StatusCode, msg: T) -> Result<Response<Body>, Error>
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

async fn http_service_try(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("http_service_try  {:?}", req.uri());
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/4/node_status" {
        if req.method() == Method::GET {
            Ok(node_status(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/search/channel" {
        if req.method() == Method::GET {
            Ok(search::channel_search(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/events" {
        if req.method() == Method::GET {
            Ok(plain_events(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/binned" {
        if req.method() == Method::GET {
            Ok(binned(req, node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/prebinned" {
        if req.method() == Method::GET {
            Ok(prebinned(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/table_sizes" {
        if req.method() == Method::GET {
            Ok(table_sizes(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/random_channel" {
        if req.method() == Method::GET {
            Ok(random_channel(req, &node_config).await?)
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
            Ok(clear_cache_all(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/update_db_with_channel_names" {
        if req.method() == Method::GET {
            Ok(update_db_with_channel_names(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/update_db_with_all_channel_configs" {
        if req.method() == Method::GET {
            Ok(update_db_with_all_channel_configs(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/update_search_cache" {
        if req.method() == Method::GET {
            Ok(update_search_cache(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/ca_connect_1" {
        if req.method() == Method::GET {
            Ok(ca_connect_1(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/archapp/files/scan" {
        if req.method() == Method::GET {
            Ok(archapp_scan_files(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/archapp/channel/info" {
        if req.method() == Method::GET {
            Ok(archapp_channel_info(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/channel/config" {
        if req.method() == Method::GET {
            Ok(channel_config(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/1/query" {
        if req.method() == Method::POST {
            Ok(api1::api1_binary_events(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if pulsemap::IndexFullHttpFunction::path_matches(path) {
        pulsemap::IndexFullHttpFunction::handle(req, &node_config).await
    } else if pulsemap::MarkClosedHttpFunction::path_matches(path) {
        pulsemap::MarkClosedHttpFunction::handle(req, &node_config).await
    } else if pulsemap::MapPulseLocalHttpFunction::path_matches(path) {
        pulsemap::MapPulseLocalHttpFunction::handle(req, &node_config).await
    } else if pulsemap::MapPulseHistoHttpFunction::path_matches(path) {
        pulsemap::MapPulseHistoHttpFunction::handle(req, &node_config).await
    } else if pulsemap::MapPulseHttpFunction::path_matches(path) {
        pulsemap::MapPulseHttpFunction::handle(req, &node_config).await
    } else if let Some(h) = channelarchiver::ListIndexFilesHttpFunction::should_handle(path) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelarchiver::ListChannelsHttpFunction::should_handle(path) {
        h.handle(req, &node_config).await
    } else if path.starts_with("/api/1/requestStatus/") {
        info!("{}", path);
        Ok(response(StatusCode::OK).body(Body::from("{}"))?)
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
        Ok(response(StatusCode::NOT_FOUND).body(Body::from(format!(
            "Sorry, not found: {:?}  {:?}  {:?}",
            req.method(),
            req.uri().path(),
            req.uri().query(),
        )))?)
    }
}

pub fn api_4_docs(path: &str) -> Result<Response<Body>, Error> {
    static_http!(path, "", "api4.html", "text/html");
    static_http!(path, "style.css", "text/css");
    static_http!(path, "script.js", "text/javascript");
    static_http!(path, "status-main.html", "text/html");
    Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
}

pub fn api_1_docs(path: &str) -> Result<Response<Body>, Error> {
    static_http_api1!(path, "", "api1.html", "text/html");
    static_http_api1!(path, "style.css", "text/css");
    static_http_api1!(path, "script.js", "text/javascript");
    Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
}

fn response<T>(status: T) -> http::response::Builder
where
    http::StatusCode: std::convert::TryFrom<T>,
    <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder()
        .status(status)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "*")
        .header("x-proxy-log-mark", proxy_mark())
}

struct BodyStreamWrap(netpod::BodyStream);

impl hyper::body::HttpBody for BodyStreamWrap {
    type Data = bytes::Bytes;
    type Error = Error;

    fn poll_data(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        self.0.inner.poll_next_unpin(cx)
    }

    fn poll_trailers(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(None))
    }
}

struct BodyStream<S> {
    inp: S,
    desc: String,
}

impl<S, I> BodyStream<S>
where
    S: Stream<Item = Result<I, Error>> + Unpin + Send + 'static,
    I: Into<Bytes> + Sized + 'static,
{
    pub fn new(inp: S, desc: String) -> Self {
        Self { inp, desc }
    }

    pub fn wrapped(inp: S, desc: String) -> Body {
        Body::wrap_stream(Self::new(inp, desc))
    }
}

impl<S, I> Stream for BodyStream<S>
where
    S: Stream<Item = Result<I, Error>> + Unpin,
    I: Into<Bytes> + Sized,
{
    type Item = Result<I, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let span1 = span!(Level::INFO, "httpret::BodyStream", desc = Empty);
        span1.record("desc", &self.desc.as_str());
        span1.in_scope(|| {
            use Poll::*;
            let t = std::panic::catch_unwind(AssertUnwindSafe(|| self.inp.poll_next_unpin(cx)));
            match t {
                Ok(r) => match r {
                    Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
                    Ready(Some(Err(e))) => {
                        error!("body stream error: {:?}", e);
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => Ready(None),
                    Pending => Pending,
                },
                Err(e) => {
                    error!("panic caught in httpret::BodyStream: {:?}", e);
                    let e = Error::with_msg(format!("panic caught in httpret::BodyStream: {:?}", e));
                    Ready(Some(Err(e)))
                }
            }
        })
    }
}

async fn binned(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let url = Url::parse(&format!("dummy:{}", head.uri))?;
    let query = BinnedQuery::from_url(&url)?;
    let desc = format!("binned-BEG-{}-END-{}", query.range().beg / SEC, query.range().end / SEC);
    let span1 = span!(Level::INFO, "httpret::binned", desc = &desc.as_str());
    span1.in_scope(|| {
        info!("binned STARTING  {:?}", query);
    });
    match head.headers.get(http::header::ACCEPT) {
        Some(v) if v == APP_OCTET => binned_binary(query, node_config).await,
        Some(v) if v == APP_JSON => binned_json(query, node_config).await,
        _ => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

async fn binned_binary(query: BinnedQuery, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let ret = match disk::binned::binned_bytes_for_http(&query, node_config).await {
        Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(s, format!("binned_binary")))?,
        Err(e) => {
            if query.report_error() {
                response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("{:?}", e)))?
            } else {
                error!("fn binned_binary: {:?}", e);
                response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?
            }
        }
    };
    Ok(ret)
}

async fn binned_json(query: BinnedQuery, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let ret = match disk::binned::binned_json(&query, node_config).await {
        Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(s, format!("binned_json")))?,
        Err(e) => {
            if query.report_error() {
                response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("{:?}", e)))?
            } else {
                error!("fn binned_json: {:?}", e);
                response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?
            }
        }
    };
    Ok(ret)
}

async fn prebinned(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let query = PreBinnedQuery::from_request(&head)?;
    let desc = format!(
        "pre-W-{}-B-{}",
        query.patch().bin_t_len() / SEC,
        query.patch().patch_beg() / SEC
    );
    let span1 = span!(Level::INFO, "httpret::prebinned", desc = &desc.as_str());
    span1.in_scope(|| {
        info!("prebinned STARTING");
    });
    let fut = disk::binned::prebinned::pre_binned_bytes_for_http(node_config, &query).instrument(span1);
    let ret = match fut.await {
        Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(s, desc))?,
        Err(e) => {
            if query.report_error() {
                response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("{:?}", e)))?
            } else {
                error!("fn prebinned: {:?}", e);
                response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?
            }
        }
    };
    Ok(ret)
}

async fn plain_events(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("httpret  plain_events  headers: {:?}", req.headers());
    let accept_def = "";
    let accept = req
        .headers()
        .get(http::header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
    if accept == APP_JSON {
        let ret = match plain_events_json(req, node_config).await {
            Ok(ret) => ret,
            Err(e) => {
                error!("{}", e);
                response_err(StatusCode::BAD_REQUEST, e.msg())?
            }
        };
        Ok(ret)
    } else if accept == APP_OCTET {
        let ret = match plain_events_binary(req, node_config).await {
            Ok(ret) => ret,
            Err(e) => {
                error!("{}", e);
                response_err(StatusCode::BAD_REQUEST, e.msg())?
            }
        };
        Ok(ret)
    } else {
        let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("unsupported Accept: {:?}", accept))?;
        Ok(ret)
    }
}

async fn plain_events_binary(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("httpret  plain_events_binary  req: {:?}", req);
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    let query = PlainEventsBinaryQuery::from_url(&url)?;
    let op = disk::channelexec::PlainEvents::new(
        query.channel().clone(),
        query.range().clone(),
        query.disk_io_buffer_size(),
        node_config.clone(),
    );
    let s = disk::channelexec::channel_exec(op, query.channel(), query.range(), AggKind::Plain, node_config).await?;
    let s = s.map(|item| item.make_frame());
    let ret = response(StatusCode::OK).body(BodyStream::wrapped(s, format!("plain_events_binary")))?;
    Ok(ret)
}

async fn plain_events_json(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("httpret  plain_events_json  req: {:?}", req);
    let (head, _body) = req.into_parts();
    let query = PlainEventsJsonQuery::from_request_head(&head)?;
    let op = disk::channelexec::PlainEventsJson::new(
        query.channel().clone(),
        query.range().clone(),
        query.disk_io_buffer_size(),
        query.timeout(),
        node_config.clone(),
        query.do_log(),
    );
    let s = disk::channelexec::channel_exec(op, query.channel(), query.range(), AggKind::Plain, node_config).await?;
    let ret = response(StatusCode::OK).body(BodyStream::wrapped(s, format!("plain_events_json")))?;
    Ok(ret)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    database_size: u64,
}

async fn node_status(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (_head, _body) = req.into_parts();
    let ret = NodeStatus {
        database_size: dbconn::database_size(node_config).await?,
    };
    let ret = serde_json::to_vec(&ret)?;
    let ret = response(StatusCode::OK).body(Body::from(ret))?;
    Ok(ret)
}

async fn table_sizes(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (_head, _body) = req.into_parts();
    let sizes = dbconn::table_sizes(node_config).await?;
    let mut ret = String::new();
    for size in sizes.sizes {
        use std::fmt::Write;
        write!(ret, "{:60} {:20}\n", size.0, size.1)?;
    }
    let ret = response(StatusCode::OK).body(Body::from(ret))?;
    Ok(ret)
}

pub async fn random_channel(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (_head, _body) = req.into_parts();
    let ret = dbconn::random_channel(node_config).await?;
    let ret = response(StatusCode::OK).body(Body::from(ret))?;
    Ok(ret)
}

pub async fn clear_cache_all(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
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

pub async fn update_db_with_channel_names(
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
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

pub async fn update_db_with_channel_names_3(
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
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

pub async fn update_db_with_all_channel_configs(
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
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

pub async fn update_search_cache(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
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

pub async fn channel_config(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("channel_config");
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    //let pairs = get_url_query_pairs(&url);
    let q = ChannelConfigQuery::from_url(&url)?;
    info!("ChannelConfigQuery {:?}", q);
    let conf = if let Some(conf) = &node_config.node.channel_archiver {
        archapp_wrap::archapp::archeng::channel_config(&q, conf).await?
    } else if let Some(conf) = &node_config.node.archiver_appliance {
        archapp_wrap::channel_config(&q, conf).await?
    } else {
        parse::channelconfig::channel_config(&q, &node_config.node).await?
    };
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON)
        .body(Body::from(serde_json::to_string(&conf)?))?;
    Ok(ret)
}

pub async fn ca_connect_1(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    let pairs = get_url_query_pairs(&url);
    let res = netfetch::ca::ca_connect_1(pairs, node_config).await?;
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

pub async fn archapp_scan_files(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    let pairs = get_url_query_pairs(&url);
    let res = archapp_wrap::scan_files(pairs, node_config.clone()).await?;
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
        .body(Body::wrap_stream(res.map(|k| match k {
            Ok(k) => match k.serialize() {
                Ok(mut item) => {
                    item.push(0xa);
                    Ok(item)
                }
                Err(e) => Err(e),
            },
            Err(e) => match serde_json::to_vec(&e) {
                Ok(mut item) => {
                    item.push(0xa);
                    Ok(item)
                }
                Err(e) => Err(e.into()),
            },
        })))?;
    Ok(ret)
}

pub async fn archapp_channel_info(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    let pairs = get_url_query_pairs(&url);
    let channel = channel_from_pairs(&pairs)?;
    match archapp_wrap::channel_info(&channel, node_config).await {
        Ok(res) => {
            let buf = serde_json::to_vec(&res)?;
            let ret = response(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, APP_JSON)
                .body(Body::from(buf))?;
            Ok(ret)
        }
        Err(e) => {
            let ret = response(StatusCode::INTERNAL_SERVER_ERROR)
                .header(http::header::CONTENT_TYPE, "text/text")
                .body(Body::from(format!("{:?}", e)))?;
            Ok(ret)
        }
    }
}
