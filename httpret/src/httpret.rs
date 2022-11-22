pub mod api1;
pub mod bodystream;
pub mod channel_status;
pub mod channelconfig;
pub mod download;
pub mod err;
pub mod events;
pub mod evinfo;
pub mod gather;
pub mod prometheus;
pub mod proxy;
pub mod pulsemap;
pub mod search;
pub mod settings;

use self::bodystream::{BodyStream, ToPublicResponse};
use crate::bodystream::response;
use crate::err::Error;
use crate::gather::gather_get_json;
use crate::pulsemap::UpdateTask;
use channelconfig::{chconf_from_binned, ChConf};
use disk::binned::query::PreBinnedQuery;
use future::Future;
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use http::{Method, StatusCode};
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{server::Server, Body, Request, Response};
use net::SocketAddr;
use netpod::log::*;
use netpod::query::BinnedQuery;
use netpod::timeunits::SEC;
use netpod::{FromUrl, NodeConfigCached, NodeStatus, NodeStatusArchiverAppliance};
use netpod::{ACCEPT_ALL, APP_JSON, APP_JSON_LINES, APP_OCTET};
use nodenet::conn::events_service;
use panic::{AssertUnwindSafe, UnwindSafe};
use pin::Pin;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Once, RwLock, RwLockWriteGuard};
use std::time::SystemTime;
use std::{future, net, panic, pin, task};
use task::{Context, Poll};
use tracing::Instrument;
use url::Url;

pub async fn host(node_config: NodeConfigCached) -> Result<(), Error> {
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
            debug!("new connection from {:?}", conn.remote_addr());
            let node_config = node_config.clone();
            let addr = conn.remote_addr();
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
            error!("daqbuffer node http_service sees error: {}", e);
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

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
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
                Poll::Ready(Err(Error::with_msg(format!("{:?}", e))))
            }
        }
    }
}

impl<F> UnwindSafe for Cont<F> {}

// TODO remove because I want error bodies to be json.
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
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/4/node_status" {
        if req.method() == Method::GET {
            Ok(node_status(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path == "/api/4/version" {
        if req.method() == Method::GET {
            let ret = serde_json::json!({
                "data_api_version": {
                    "major": 4u32,
                    "minor": 2u32,
                    "patch": 0u32,
                },
            });
            Ok(response(StatusCode::OK).body(Body::from(serde_json::to_vec(&ret)?))?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if let Some(h) = StatusBoardAllHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if path == "/api/4/search/channel" {
        if req.method() == Method::GET {
            Ok(search::channel_search(req, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if let Some(h) = channelconfig::ChannelConfigHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::ScyllaChannelEventSeriesId::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::ScyllaConfigsHisto::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::ScyllaChannelsWithType::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::IocForChannel::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::ScyllaChannelsActive::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::ScyllaSeriesTsMsp::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::ChannelFromSeries::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::AmbigiousChannelNames::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channelconfig::GenerateScyllaTestData::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = events::EventsHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = events::EventsHandlerScylla::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = events::BinnedHandlerScylla::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channel_status::ConnectionStatusEvents::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = channel_status::ChannelConnectionStatusEvents::handler(&req) {
        h.handle(req, &node_config).await
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
    } else if path == "/api/4/random/channel" {
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
    } else if let Some(h) = download::DownloadHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = settings::SettingsThreadsMaxHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = api1::Api1EventsBinaryHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = evinfo::EventInfoScan::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = pulsemap::MapPulseScyllaHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = pulsemap::IndexFullHttpFunction::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = pulsemap::MarkClosedHttpFunction::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = pulsemap::MapPulseLocalHttpFunction::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = pulsemap::MapPulseHistoHttpFunction::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = pulsemap::MapPulseHttpFunction::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = pulsemap::Api4MapPulseHttpFunction::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = api1::RequestStatusHandler::handler(&req) {
        h.handle(req, &node_config).await
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

pub struct StatusBoardAllHandler {}

impl StatusBoardAllHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/status/board/all" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, _req: Request<Body>, _node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        use std::ops::Deref;
        let sb = status_board().unwrap();
        let buf = serde_json::to_vec(sb.deref()).unwrap();
        let res = response(StatusCode::OK).body(Body::from(buf))?;
        Ok(res)
    }
}

async fn binned(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    match binned_inner(req, node_config).await {
        Ok(ret) => Ok(ret),
        Err(e) => {
            error!("fn binned: {e:?}");
            Ok(e.to_public_response())
        }
    }
}

async fn binned_inner(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let url = Url::parse(&format!("dummy:{}", head.uri))?;
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        let msg = format!("can not parse query: {}", e.msg());
        e.add_public_msg(msg)
    })?;
    let chconf = chconf_from_binned(&query, node_config).await?;
    // Update the series id since we don't require some unique identifier yet.
    let mut query = query;
    query.set_series_id(chconf.series);
    let query = query;
    // ---
    let desc = format!("binned-BEG-{}-END-{}", query.range().beg / SEC, query.range().end / SEC);
    let span1 = span!(Level::INFO, "httpret::binned", desc = &desc.as_str());
    span1.in_scope(|| {
        debug!("binned STARTING  {:?}", query);
    });
    match head.headers.get(http::header::ACCEPT) {
        Some(v) if v == APP_OCTET => binned_binary(query, chconf, node_config).await,
        Some(v) if v == APP_JSON || v == ACCEPT_ALL => binned_json(query, chconf, node_config).await,
        _ => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

async fn binned_binary(
    query: BinnedQuery,
    chconf: ChConf,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
    let body_stream =
        disk::binned::binned_bytes_for_http(&query, chconf.scalar_type, chconf.shape, node_config).await?;
    let res = response(StatusCode::OK).body(BodyStream::wrapped(
        body_stream.map_err(Error::from),
        format!("binned_binary"),
    ))?;
    Ok(res)
}

async fn binned_json(
    query: BinnedQuery,
    chconf: ChConf,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
    let body_stream = disk::binned::binned_json(&query, chconf.scalar_type, chconf.shape, node_config).await?;
    let res = response(StatusCode::OK).body(BodyStream::wrapped(
        body_stream.map_err(Error::from),
        format!("binned_json"),
    ))?;
    Ok(res)
}

async fn prebinned(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    match prebinned_inner(req, node_config).await {
        Ok(ret) => Ok(ret),
        Err(e) => {
            error!("fn prebinned: {e:?}");
            Ok(response(StatusCode::BAD_REQUEST).body(Body::from(e.msg().to_string()))?)
        }
    }
}

async fn prebinned_inner(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let query = PreBinnedQuery::from_request(&head)?;
    let desc = format!(
        "pre-W-{}-B-{}",
        query.patch().bin_t_len() / SEC,
        query.patch().patch_beg() / SEC
    );
    let span1 = span!(Level::INFO, "httpret::prebinned", desc = &desc.as_str());
    span1.in_scope(|| {
        debug!("prebinned STARTING");
    });
    let fut = disk::binned::prebinned::pre_binned_bytes_for_http(node_config, &query).instrument(span1);
    let ret = match fut.await {
        Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(s.map_err(Error::from), desc))?,
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

async fn node_status(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (_head, _body) = req.into_parts();
    let archiver_appliance_status = match node_config.node.archiver_appliance.as_ref() {
        Some(k) => {
            let mut st = vec![];
            for p in &k.data_base_paths {
                let _m = match tokio::fs::metadata(p).await {
                    Ok(m) => m,
                    Err(_e) => {
                        st.push((p.into(), false));
                        continue;
                    }
                };
                let _ = match tokio::fs::read_dir(p).await {
                    Ok(rd) => rd,
                    Err(_e) => {
                        st.push((p.into(), false));
                        continue;
                    }
                };
                st.push((p.into(), true));
            }
            Some(NodeStatusArchiverAppliance { readable: st })
        }
        None => None,
    };
    let database_size = dbconn::database_size(node_config).await.map_err(|e| format!("{e:?}"));
    let ret = NodeStatus {
        is_sf_databuffer: node_config.node.sf_databuffer.is_some(),
        is_archiver_engine: node_config.node.channel_archiver.is_some(),
        is_archiver_appliance: node_config.node.archiver_appliance.is_some(),
        database_size,
        archiver_appliance_status,
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

#[derive(Serialize)]
pub struct StatusBoardEntry {
    #[allow(unused)]
    #[serde(serialize_with = "instant_serde::ser")]
    ts_created: SystemTime,
    #[serde(serialize_with = "instant_serde::ser")]
    ts_updated: SystemTime,
    #[serde(skip_serializing_if = "items::bool_is_false")]
    is_error: bool,
    #[serde(skip_serializing_if = "items::bool_is_false")]
    is_ok: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<Error>,
}

mod instant_serde {
    use super::*;
    use serde::Serializer;
    pub fn ser<S: Serializer>(x: &SystemTime, ser: S) -> Result<S::Ok, S::Error> {
        use chrono::LocalResult;
        let dur = x.duration_since(std::time::UNIX_EPOCH).unwrap();
        let res = chrono::TimeZone::timestamp_opt(&chrono::Utc, dur.as_secs() as i64, dur.subsec_nanos());
        match res {
            LocalResult::None => Err(serde::ser::Error::custom(format!("Bad local instant conversion"))),
            LocalResult::Single(dt) => {
                let s = dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
                ser.serialize_str(&s)
            }
            LocalResult::Ambiguous(dt, _dt2) => {
                let s = dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
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
            is_error: false,
            is_ok: false,
            errors: vec![],
        }
    }
}

#[derive(Serialize)]
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
        let mut buf = [0; 8];
        f.read_exact(&mut buf).unwrap();
        let n = u64::from_le_bytes(buf);
        let s = format!("{:016x}", n);
        info!("new_status_id {s}");
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

    pub fn mark_ok(&mut self, status_id: &str) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
                if !e.is_error {
                    e.is_ok = true;
                }
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn add_error(&mut self, status_id: &str, error: Error) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
                e.is_error = true;
                e.is_ok = false;
                e.errors.push(error);
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn status_as_json(&self, status_id: &str) -> String {
        #[derive(Serialize)]
        struct StatJs {
            #[serde(skip_serializing_if = "Vec::is_empty")]
            errors: Vec<::err::PublicError>,
        }
        match self.entries.get(status_id) {
            Some(e) => {
                if e.is_ok {
                    let js = StatJs { errors: vec![] };
                    return serde_json::to_string(&js).unwrap();
                } else if e.is_error {
                    let errors = e.errors.iter().map(|e| (&e.0).into()).collect();
                    let js = StatJs { errors };
                    return serde_json::to_string(&js).unwrap();
                } else {
                    warn!("requestStatus for unfinished {status_id}");
                    let js = StatJs { errors: vec![] };
                    return serde_json::to_string(&js).unwrap();
                }
            }
            None => {
                error!("can not find status id {}", status_id);
                let e = ::err::Error::with_public_msg_no_trace(format!("Request status ID unknown {status_id}"));
                let js = StatJs { errors: vec![e.into()] };
                return serde_json::to_string(&js).unwrap();
            }
        }
    }
}

static STATUS_BOARD: AtomicPtr<RwLock<StatusBoard>> = AtomicPtr::new(std::ptr::null_mut());

pub fn status_board() -> Result<RwLockWriteGuard<'static, StatusBoard>, Error> {
    let x = unsafe { &*STATUS_BOARD.load(Ordering::SeqCst) }.write();
    match x {
        Ok(x) => Ok(x),
        Err(e) => Err(Error::with_msg(format!("{e:?}"))),
    }
}
