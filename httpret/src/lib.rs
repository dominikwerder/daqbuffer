use crate::gather::gather_get_json;
use bytes::Bytes;
use disk::binned::prebinned::pre_binned_bytes_for_http;
use disk::binned::query::{BinnedQuery, PreBinnedQuery};
use disk::binned::BinnedStreamKindScalar;
use disk::raw::conn::events_service;
use err::Error;
use future::Future;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use http::{HeaderMap, Method, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::{server::Server, Body, Request, Response};
use net::SocketAddr;
use netpod::log::*;
use netpod::{Channel, NodeConfigCached};
use panic::{AssertUnwindSafe, UnwindSafe};
use pin::Pin;
use serde::{Deserialize, Serialize};
use std::{future, net, panic, pin, task};
use task::{Context, Poll};
use tracing::field::Empty;
use tracing::Instrument;

pub mod gather;
pub mod search;

pub async fn host(node_config: NodeConfigCached) -> Result<(), Error> {
    let node_config = node_config.clone();
    let rawjh = taskrun::spawn(events_service(node_config.clone()));
    use std::str::FromStr;
    let addr = SocketAddr::from_str(&format!("{}:{}", node_config.node.listen, node_config.node.port))?;
    let make_service = make_service_fn({
        move |_conn| {
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
            error!("data_api_proxy sees error: {:?}", e);
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
            // TODO multi-facility search
            Ok(search::channel_search(req, &node_config).await?)
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
    } else if path == "/api/4/channel/config" {
        if req.method() == Method::GET {
            Ok(channel_config(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with("/api/1/documentation/") {
        if req.method() == Method::GET {
            static_http_api1!(path, "", "api1.html", "text/html");
            static_http_api1!(path, "style.css", "text/css");
            static_http_api1!(path, "script.js", "text/javascript");
            Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with("/api/4/documentation/") {
        if req.method() == Method::GET {
            static_http!(path, "", "api4.html", "text/html");
            static_http!(path, "style.css", "text/css");
            static_http!(path, "script.js", "text/javascript");
            static_http!(path, "status-main.html", "text/html");
            Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
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

fn response<T>(status: T) -> http::response::Builder
where
    http::StatusCode: std::convert::TryFrom<T>,
    <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder()
        .status(status)
        .header("access-control-allow-origin", "*")
        .header("access-control-allow-headers", "*")
}

struct BodyStreamWrap(netpod::BodyStream);

impl hyper::body::HttpBody for BodyStreamWrap {
    type Data = bytes::Bytes;
    type Error = Error;

    fn poll_data(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        todo!()
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
    let query = BinnedQuery::from_request(&head)?;
    match head.headers.get("accept") {
        Some(v) if v == "application/octet-stream" => binned_binary(query, node_config).await,
        Some(v) if v == "application/json" => binned_json(query, node_config).await,
        _ => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

async fn binned_binary(query: BinnedQuery, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let ret = match disk::binned::binned_bytes_for_http(&query, node_config).await {
        Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(s, format!("desc-BINNED")))?,
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
    let ret = match disk::binned::binned_json(node_config, &query).await {
        Ok(val) => {
            let body = serde_json::to_string(&val)?;
            response(StatusCode::OK).body(Body::from(body))
        }?,
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
    let desc = format!("pre-b-{}", query.patch().bin_t_len() / 1000000000);
    let span1 = span!(Level::INFO, "httpret::prebinned_DISABLED", desc = &desc.as_str());
    // TODO remove StreamKind
    let stream_kind = BinnedStreamKindScalar::new();
    //span1.in_scope(|| {});
    let fut = pre_binned_bytes_for_http(node_config, &query, stream_kind).instrument(span1);
    let ret = match fut.await {
        Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(
            s,
            format!(
                "pre-b-{}-p-{}",
                query.patch().bin_t_len() / 1000000000,
                query.patch().patch_beg() / 1000000000,
            ),
        ))?,
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
        .header(http::header::CONTENT_TYPE, "application/json")
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
            .await?;
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/jsonlines")
        .body(Body::wrap_stream(res.map(|k| match serde_json::to_string(&k) {
            Ok(mut item) => {
                item.push('\n');
                Ok(item)
            }
            Err(e) => Err(e),
        })))?;
    Ok(ret)
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
        .header(http::header::CONTENT_TYPE, "application/jsonlines")
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
        .header(http::header::CONTENT_TYPE, "application/jsonlines")
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
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_string(&res)?))?;
    Ok(ret)
}

pub async fn channel_config(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let _dry = match head.uri.query() {
        Some(q) => q.contains("dry"),
        None => false,
    };
    let params = netpod::query_params(head.uri.query());
    let channel = Channel {
        backend: node_config.node.backend.clone(),
        name: params.get("channelName").unwrap().into(),
    };
    let res = parse::channelconfig::read_local_config(&channel, &node_config.node).await?;
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_string(&res)?))?;
    Ok(ret)
}

pub async fn ca_connect_1(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let params = netpod::query_params(head.uri.query());
    let addr = params.get("addr").unwrap().into();
    let res = netfetch::ca_connect_1(addr, node_config).await?;
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/jsonlines")
        .body(Body::wrap_stream(res.map(|k| match serde_json::to_string(&k) {
            Ok(mut item) => {
                item.push('\n');
                Ok(item)
            }
            Err(e) => Err(e),
        })))?;
    Ok(ret)
}
