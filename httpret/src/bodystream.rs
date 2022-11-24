use crate::err::Error;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use http::HeaderMap;
use http::{Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::APP_JSON;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::field::Empty;
use tracing::{span, Level};

pub fn response<T>(status: T) -> http::response::Builder
where
    http::StatusCode: std::convert::TryFrom<T>,
    <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder().status(status)
}

pub struct BodyStream<S> {
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
                        error!("body stream error: {e:?}");
                        Ready(Some(Err(Error::from(e))))
                    }
                    Ready(None) => Ready(None),
                    Pending => Pending,
                },
                Err(e) => {
                    error!("panic caught in httpret::BodyStream: {e:?}");
                    let e = Error::with_msg(format!("panic caught in httpret::BodyStream: {e:?}"));
                    Ready(Some(Err(e)))
                }
            }
        })
    }
}

pub trait ToPublicResponse {
    fn to_public_response(&self) -> Response<Body>;
}

impl ToPublicResponse for Error {
    fn to_public_response(&self) -> Response<Body> {
        self.0.to_public_response()
    }
}

impl ToPublicResponse for ::err::Error {
    fn to_public_response(&self) -> Response<Body> {
        use err::Reason;
        let e = self.to_public_error();
        let status = match e.reason() {
            Some(Reason::BadRequest) => StatusCode::BAD_REQUEST,
            Some(Reason::InternalError) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let msg = match serde_json::to_string(&e) {
            Ok(s) => s,
            Err(_) => "can not serialize error".into(),
        };
        match response(status)
            .header(http::header::ACCEPT, APP_JSON)
            .body(Body::from(msg))
        {
            Ok(res) => res,
            Err(e) => {
                error!("can not generate http error response {e:?}");
                let mut res = Response::new(Body::default());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                res
            }
        }
    }
}

struct BodyStreamWrap(netpod::BodyStream);

impl hyper::body::HttpBody for BodyStreamWrap {
    type Data = bytes::Bytes;
    type Error = ::err::Error;

    fn poll_data(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        self.0.inner.poll_next_unpin(cx)
    }

    fn poll_trailers(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(None))
    }
}
