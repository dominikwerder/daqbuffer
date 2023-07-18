use err::Error;
use futures_util::StreamExt;
use http::HeaderMap;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::APP_JSON;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub fn response<T>(status: T) -> http::response::Builder
where
    http::StatusCode: std::convert::TryFrom<T>,
    <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder().status(status)
}

pub trait ToPublicResponse {
    fn to_public_response(&self) -> Response<Body>;
}

impl ToPublicResponse for Error {
    fn to_public_response(&self) -> Response<Body> {
        self.0.to_public_response()
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
