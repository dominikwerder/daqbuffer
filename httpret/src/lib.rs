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
//use std::pin::Pin;
//use std::future::Future;
//use serde_derive::{Serialize, Deserialize};
//use serde_json::{Value as SerdeValue, Value as JsonValue};

pub async fn host(port: u16) -> Result<(), Error> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let make_service = make_service_fn(|_conn| async {
        Ok::<_, Error>(service_fn(data_api_proxy))
    });
    Server::bind(&addr).serve(make_service).await?;
    Ok(())
}

async fn data_api_proxy(req: Request<Body>) -> Result<Response<Body>, Error> {
    match data_api_proxy_try(req).await {
        Ok(k) => { Ok(k) }
        Err(e) => {
            error!("{:?}", e);
            Err(e)
        }
    }
}

async fn data_api_proxy_try(req: Request<Body>) -> Result<Response<Body>, Error> {
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
    use netpod::AggQuerySingleChannel;
    let reqbody = req.into_body();
    let bodyslice = hyper::body::to_bytes(reqbody).await?;
    let _query: AggQuerySingleChannel = serde_json::from_slice(&bodyslice)?;
    let q = disk::read_test_1().await?;
    let s = q.inner;
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
