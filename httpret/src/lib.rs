#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::net::SocketAddr;
use http::{Method, StatusCode, HeaderMap};
use hyper::{Body, Request, Response};
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use std::task::{Context, Poll};
use std::pin::Pin;
use netpod::{Node, Cluster, AggKind};
use disk::cache::BinParams;

pub async fn host(node: Node, cluster: Cluster) -> Result<(), Error> {
    let addr = SocketAddr::from(([0, 0, 0, 0], node.port));
    let make_service = make_service_fn({
        move |_conn| {
            let node = node.clone();
            let cluster = cluster.clone();
            async move {
                Ok::<_, Error>(service_fn({
                    move |req| {
                        let hc = HostConf {
                            node: node.clone(),
                            cluster: cluster.clone(),
                        };
                        data_api_proxy(req, hc)
                    }
                }))
            }
        }
    });
    Server::bind(&addr).serve(make_service).await?;
    Ok(())
}

async fn data_api_proxy(req: Request<Body>, hconf: HostConf) -> Result<Response<Body>, Error> {
    match data_api_proxy_try(req, hconf).await {
        Ok(k) => { Ok(k) }
        Err(e) => {
            error!("{:?}", e);
            Err(e)
        }
    }
}

async fn data_api_proxy_try(req: Request<Body>, hconf: HostConf) -> Result<Response<Body>, Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/1/parsed_raw" {
        if req.method() == Method::POST {
            Ok(parsed_raw(req).await?)
        }
        else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
    else if path == "/api/1/binned" {
        if req.method() == Method::GET {
            Ok(binned(req, hconf).await?)
        }
        else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
    else {
        Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
    }
}

fn response<T>(status: T) -> http::response::Builder
    where
        http::StatusCode: std::convert::TryFrom<T>,
        <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder().status(status)
        .header("access-control-allow-origin", "*")
        .header("access-control-allow-headers", "*")
}


async fn parsed_raw(req: Request<Body>) -> Result<Response<Body>, Error> {
    let node = todo!("get node from config");
    use netpod::AggQuerySingleChannel;
    let reqbody = req.into_body();
    let bodyslice = hyper::body::to_bytes(reqbody).await?;
    let query: AggQuerySingleChannel = serde_json::from_slice(&bodyslice)?;
    //let q = disk::read_test_1(&query).await?;
    //let s = q.inner;
    let s = disk::parsed1(&query, &node);
    let res = response(StatusCode::OK)
        .body(Body::wrap_stream(s))?;
    /*
    let res = match q {
        Ok(k) => {
            response(StatusCode::OK)
                .body(Body::wrap_stream(k.inner))?
        }
        Err(e) => {
            response(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())?
        }
    };
    */
    Ok(res)
}


struct BodyStreamWrap(netpod::BodyStream);

impl hyper::body::HttpBody for BodyStreamWrap {
    type Data = bytes::Bytes;
    type Error = Error;

    fn poll_data(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        /*
        use futures_core::stream::Stream;
        let z: &mut async_channel::Receiver<Result<Self::Data, Self::Error>> = &mut self.0.receiver;
        match Pin::new(z).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(k) => Poll::Ready(k),
        }
        */
        todo!()
    }

    fn poll_trailers(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(None))
    }

}


async fn binned(req: Request<Body>, hconf: HostConf) -> Result<Response<Body>, Error> {
    let (head, body) = req.into_parts();
    let params = netpod::query_params(head.uri.query());

    // TODO
    // Channel, time range, bin size.
    // Try to locate that file in cache, otherwise create it on the fly:
    // Look up and parse channel config.
    // Extract the relevant channel config entry.

    let query = disk::cache::Query::from_request(&head)?;
    let params = BinParams {
        node: hconf.node.clone(),
        cluster: hconf.cluster.clone(),
    };
    let ret = match disk::cache::binned_bytes_for_http(params, &query) {
        Ok(s) => {
            response(StatusCode::OK)
            .body(Body::wrap_stream(s))?
        }
        Err(e) => {
            error!("{:?}", e);
            response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?
        }
    };
    Ok(ret)
}



#[derive(Clone)]
pub struct HostConf {
    node: Node,
    cluster: Cluster,
}
