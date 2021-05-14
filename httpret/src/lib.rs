use bytes::Bytes;
use disk::cache::{BinnedQuery, PreBinnedQuery};
use disk::eventchunker::EventChunkerConf;
use disk::raw::conn::raw_service;
use err::Error;
use future::Future;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use http::{HeaderMap, Method, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::{server::Server, Body, Request, Response};
use net::SocketAddr;
use netpod::{ByteSize, Node, NodeConfigCached};
use panic::{AssertUnwindSafe, UnwindSafe};
use pin::Pin;
use serde::{Deserialize, Serialize};
use std::{future, net, panic, pin, task};
use task::{Context, Poll};
use tracing::field::Empty;
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

pub async fn host(node_config: NodeConfigCached) -> Result<(), Error> {
    let node_config = node_config.clone();
    let rawjh = taskrun::spawn(raw_service(node_config.clone()));
    use std::str::FromStr;
    let addr = SocketAddr::from_str(&format!("{}:{}", node_config.node.listen, node_config.node.port))?;
    let make_service = make_service_fn({
        move |_conn| {
            let node_config = node_config.clone();
            async move {
                Ok::<_, Error>(service_fn({
                    move |req| {
                        let f = data_api_proxy(req, node_config.clone());
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

async fn data_api_proxy(req: Request<Body>, node_config: NodeConfigCached) -> Result<Response<Body>, Error> {
    match data_api_proxy_try(req, &node_config).await {
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
    ($path:expr, $tgt:expr, $tgtex:expr) => {
        if $path == concat!("/api/4/documentation/", $tgt) {
            let c = include_bytes!(concat!("../static/documentation/", $tgt, $tgtex));
            return Ok(response(StatusCode::OK).body(Body::from(&c[..]))?);
        }
    };
    ($path:expr, $tgt:expr) => {
        if $path == concat!("/api/4/documentation/", $tgt) {
            let c = include_bytes!(concat!("../static/documentation/", $tgt));
            return Ok(response(StatusCode::OK).body(Body::from(&c[..]))?);
        }
    };
}

async fn data_api_proxy_try(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/4/node_status" {
        if req.method() == Method::GET {
            Ok(node_status(req, &node_config).await?)
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
    } else if path == "/api/4/parsed_raw" {
        if req.method() == Method::POST {
            Ok(parsed_raw(req, &node_config.node).await?)
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
    } else if path.starts_with("/api/4/documentation/") {
        if req.method() == Method::GET {
            static_http!(path, "", "index.html");
            static_http!(path, "page.css");
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

async fn parsed_raw(req: Request<Body>, node: &Node) -> Result<Response<Body>, Error> {
    use netpod::AggQuerySingleChannel;
    let reqbody = req.into_body();
    let bodyslice = hyper::body::to_bytes(reqbody).await?;
    let query: AggQuerySingleChannel = serde_json::from_slice(&bodyslice)?;
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    let s = disk::parsed1(&query, node, event_chunker_conf);
    let res = response(StatusCode::OK).body(Body::wrap_stream(s))?;
    Ok(res)
}

struct BodyStreamWrap(netpod::BodyStream);

impl hyper::body::HttpBody for BodyStreamWrap {
    type Data = bytes::Bytes;
    type Error = Error;

    fn poll_data(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        todo!()
    }

    fn poll_trailers(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
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
    let query = disk::cache::BinnedQuery::from_request(&head)?;
    match head.headers.get("accept") {
        Some(v) if v == "application/octet-stream" => binned_binary(query, node_config).await,
        _ => binned_json(query, node_config).await,
    }
}

async fn binned_binary(query: BinnedQuery, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("binned_binary");
    let ret = match disk::binned::binned_bytes_for_http(node_config, &query).await {
        Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(s, format!("desc-BINNED")))?,
        Err(e) => {
            error!("fn binned: {:?}", e);
            response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?
        }
    };
    Ok(ret)
}

async fn binned_json(query: BinnedQuery, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("binned_json");
    let ret = match disk::binned::binned_json(node_config, &query).await {
        Ok(val) => {
            let body = serde_json::to_string(&val)?;
            response(StatusCode::OK).body(Body::from(body))
        }?,
        Err(e) => {
            error!("fn binned: {:?}", e);
            response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?
        }
    };
    Ok(ret)
}

async fn prebinned(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let q = PreBinnedQuery::from_request(&head)?;
    let desc = format!("pre-b-{}", q.patch().bin_t_len() / 1000000000);
    let span1 = span!(Level::INFO, "httpret::prebinned", desc = &desc.as_str());
    span1.in_scope(|| {
        let ret = match disk::cache::pre_binned_bytes_for_http(node_config, &q) {
            Ok(s) => response(StatusCode::OK).body(BodyStream::wrapped(
                s,
                format!(
                    "pre-b-{}-p-{}",
                    q.patch().bin_t_len() / 1000000000,
                    q.patch().patch_beg() / 1000000000,
                ),
            ))?,
            Err(e) => {
                error!("fn prebinned: {:?}", e);
                response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?
            }
        };
        Ok(ret)
    })
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
