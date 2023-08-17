use crate::response;
use crate::response_err;
use crate::ReqCtx;
use err::thiserror;
use err::ThisError;
use err::ToPublicError;
use futures_util::TryStreamExt;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::ServiceVersion;

#[derive(Debug, ThisError)]
pub enum EventDataError {
    HttpBadMethod,
    HttpBadAccept,
    QueryParse,
    #[error("Error({0})")]
    Error(Box<dyn ToPublicError>),
    InternalError,
}

impl ToPublicError for EventDataError {
    fn to_public_error(&self) -> String {
        match self {
            EventDataError::HttpBadMethod => format!("{self}"),
            EventDataError::HttpBadAccept => format!("{self}"),
            EventDataError::QueryParse => format!("{self}"),
            EventDataError::Error(e) => e.to_public_error(),
            EventDataError::InternalError => format!("{self}"),
        }
    }
}

pub struct EventDataHandler {}

impl EventDataHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().eq("/api/4/private/eventdata/frames") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Request<Body>,
        _ctx: &ReqCtx,
        ncc: &NodeConfigCached,
        _service_version: &ServiceVersion,
    ) -> Result<Response<Body>, EventDataError> {
        if req.method() != Method::POST {
            Ok(response(StatusCode::NOT_ACCEPTABLE)
                .body(Body::empty())
                .map_err(|_| EventDataError::InternalError)?)
        } else {
            match Self::handle_req(req, ncc).await {
                Ok(ret) => Ok(ret),
                Err(e) => {
                    error!("{e}");
                    let res = response_err(StatusCode::NOT_ACCEPTABLE, e.to_public_error())
                        .map_err(|_| EventDataError::InternalError)?;
                    Ok(res)
                }
            }
        }
    }

    async fn handle_req(req: Request<Body>, ncc: &NodeConfigCached) -> Result<Response<Body>, EventDataError> {
        let (_head, body) = req.into_parts();
        let frames =
            nodenet::conn::events_get_input_frames(body.map_err(|e| err::Error::with_msg_no_trace(e.to_string())))
                .await
                .map_err(|_| EventDataError::InternalError)?;
        let (evsubq,) = nodenet::conn::events_parse_input_query(frames).map_err(|_| EventDataError::QueryParse)?;
        let stream = nodenet::conn::create_response_bytes_stream(evsubq, ncc)
            .await
            .map_err(|e| EventDataError::Error(Box::new(e)))?;
        let ret = response(StatusCode::OK)
            .body(Body::wrap_stream(stream))
            .map_err(|_| EventDataError::InternalError)?;
        Ok(ret)
    }
}
