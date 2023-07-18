use crate::err::Error;
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
