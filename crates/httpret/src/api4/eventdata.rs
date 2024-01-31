use crate::bodystream::response_err_msg;
use crate::response;
use crate::ReqCtx;
use err::thiserror;
use err::PublicError;
use err::ThisError;
use err::ToPublicError;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::read_body_bytes;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::ServiceVersion;

#[derive(Debug, ThisError)]
pub enum EventDataError {
    QueryParse,
    #[error("Error({0})")]
    Error(Box<dyn ToPublicError>),
    InternalError,
}

impl ToPublicError for EventDataError {
    fn to_public_error(&self) -> PublicError {
        match self {
            EventDataError::QueryParse => format!("{self}").into(),
            EventDataError::Error(e) => e.to_public_error(),
            EventDataError::InternalError => format!("{self}").into(),
        }
    }
}

pub struct EventDataHandler {}

impl EventDataHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().eq("/api/4/private/eventdata/frames") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        ncc: &NodeConfigCached,
        _service_version: &ServiceVersion,
    ) -> Result<StreamResponse, EventDataError> {
        if req.method() != Method::POST {
            Ok(response(StatusCode::NOT_ACCEPTABLE)
                .body(body_empty())
                .map_err(|_| EventDataError::InternalError)?)
        } else {
            match Self::handle_req(req, ncc).await {
                Ok(ret) => Ok(ret),
                Err(e) => {
                    error!("{e}");
                    let res = response_err_msg(StatusCode::NOT_ACCEPTABLE, e.to_public_error())
                        .map_err(|_| EventDataError::InternalError)?;
                    Ok(res)
                }
            }
        }
    }

    async fn handle_req(req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, EventDataError> {
        let (_head, body) = req.into_parts();
        let body = read_body_bytes(body)
            .await
            .map_err(|_e| EventDataError::InternalError)?;
        let inp = futures_util::stream::iter([Ok(body)]);
        let frames = nodenet::conn::events_get_input_frames(inp)
            .await
            .map_err(|_| EventDataError::InternalError)?;
        let (evsubq,) = nodenet::conn::events_parse_input_query(frames).map_err(|_| EventDataError::QueryParse)?;
        let stream = nodenet::conn::create_response_bytes_stream(evsubq, ncc)
            .await
            .map_err(|e| EventDataError::Error(Box::new(e)))?;
        let ret = response(StatusCode::OK)
            .body(body_stream(stream))
            .map_err(|_| EventDataError::InternalError)?;
        Ok(ret)
    }
}
