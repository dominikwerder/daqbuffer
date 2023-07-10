use crate::bodystream::response;
use crate::bodystream::BodyStream;
use crate::response_err;
use bytes::Bytes;
use err::thiserror;
use err::ToPublicError;
use futures_util::Stream;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum FindActiveError {
    #[error("HttpBadAccept")]
    HttpBadAccept,
    #[error("HttpBadUrl")]
    HttpBadUrl,
    #[error("{0}")]
    Error(Box<dyn ToPublicError>),
    #[error("{0}")]
    UrlError(#[from] url::ParseError),
    #[error("InternalError")]
    InternalError,
}

impl ToPublicError for FindActiveError {
    fn to_public_error(&self) -> String {
        match self {
            FindActiveError::HttpBadAccept => format!("{self}"),
            FindActiveError::HttpBadUrl => format!("{self}"),
            FindActiveError::Error(e) => e.to_public_error(),
            FindActiveError::UrlError(e) => format!("can not parse url: {e}"),
            FindActiveError::InternalError => format!("{self}"),
        }
    }
}

pub struct FindActiveHandler {}

impl FindActiveHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/tools/databuffer/findActive" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, ncc: &NodeConfigCached) -> Result<Response<Body>, FindActiveError> {
        if req.method() != Method::GET {
            Ok(response(StatusCode::NOT_ACCEPTABLE)
                .body(Body::empty())
                .map_err(|_| FindActiveError::InternalError)?)
        } else {
            match Self::handle_req(req, ncc).await {
                Ok(ret) => Ok(ret),
                Err(e) => {
                    error!("{e}");
                    let res = response_err(StatusCode::NOT_ACCEPTABLE, e.to_public_error())
                        .map_err(|_| FindActiveError::InternalError)?;
                    Ok(res)
                }
            }
        }
    }

    async fn handle_req(req: Request<Body>, ncc: &NodeConfigCached) -> Result<Response<Body>, FindActiveError> {
        let accept_def = APP_JSON;
        let accept = req
            .headers()
            .get(http::header::ACCEPT)
            .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
        let url = {
            let s1 = format!("dummy:{}", req.uri());
            Url::parse(&s1)?
        };
        if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
            type _A = netpod::BodyStream;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(BodyStream::wrapped(Box::pin(DummyStream::new()), "find_active".into()))
                .map_err(|_| FindActiveError::InternalError)?)
        } else {
            Err(FindActiveError::HttpBadAccept)
        }
    }
}

struct DummyStream {}

impl DummyStream {
    pub fn new() -> Self {
        todo!()
    }
}

impl Stream for DummyStream {
    type Item = Result<Bytes, crate::err::Error>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}
