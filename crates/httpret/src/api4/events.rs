use crate::channelconfig::chconf_from_events_quorum;
use crate::requests::accepts_cbor_framed;
use crate::requests::accepts_json_framed;
use crate::requests::accepts_json_or_all;
use crate::response;
use crate::ServiceSharedResources;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use err::thiserror;
use err::ThisError;
use http::header::CONTENT_TYPE;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::error_response;
use httpclient::error_status_response;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamBody;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::ChannelTypeConfigGen;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::APP_CBOR_FRAMED;
use netpod::APP_JSON;
use netpod::APP_JSON_FRAMED;
use netpod::HEADER_NAME_REQUEST_ID;
use nodenet::client::OpenBoxedBytesViaHttp;
use query::api4::events::PlainEventsQuery;
use std::sync::Arc;
use streams::collect::CollectResult;
use streams::instrument::InstrumentStream;
use streams::lenframe::bytes_chunks_to_framed;
use streams::lenframe::bytes_chunks_to_len_framed_str;
use streams::plaineventscbor::plain_events_cbor_stream;
use streams::plaineventsjson::plain_events_json_stream;
use tracing::Instrument;

#[derive(Debug, ThisError)]
#[cstm(name = "Api4Events")]
pub enum Error {
    ChannelNotFound,
    HttpLib(#[from] http::Error),
    ChannelConfig(crate::channelconfig::Error),
    Retrieval(#[from] crate::RetrievalError),
    EventsCbor(#[from] streams::plaineventscbor::Error),
    EventsJson(#[from] streams::plaineventsjson::Error),
}

impl Error {
    pub fn user_message(&self) -> String {
        match self {
            Error::ChannelNotFound => format!("channel not found"),
            _ => self.to_string(),
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::ChannelNotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn response(&self, reqid: &str) -> http::Response<StreamBody> {
        let js = serde_json::json!({
            "message": self.user_message(),
            "requestid": reqid,
        });
        if let Ok(body) = serde_json::to_string_pretty(&js) {
            match http::Response::builder()
                .status(self.status_code())
                .header(http::header::CONTENT_TYPE, APP_JSON)
                .body(httpclient::body_string(body))
            {
                Ok(res) => res,
                Err(e) => {
                    error!("can not generate http error response {e}");
                    httpclient::internal_error()
                }
            }
        } else {
            httpclient::internal_error()
        }
    }
}

impl From<crate::channelconfig::Error> for Error {
    fn from(value: crate::channelconfig::Error) -> Self {
        use crate::channelconfig::Error::*;
        match value {
            NotFound(_) => Self::ChannelNotFound,
            _ => Self::ChannelConfig(value),
        }
    }
}

pub struct EventsHandler {}

impl EventsHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, crate::err::Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?);
        }
        let self_name = "handle";
        let url = req_uri_to_url(req.uri())?;
        let evq = PlainEventsQuery::from_url(&url)?;
        debug!("{self_name}  evq {evq:?}");
        let logspan = if evq.log_level() == "trace" {
            trace!("enable trace for handler");
            tracing::span!(tracing::Level::INFO, "log_span_trace")
        } else if evq.log_level() == "debug" {
            debug!("enable debug for handler");
            tracing::span!(tracing::Level::INFO, "log_span_debug")
        } else {
            tracing::Span::none()
        };
        match plain_events(req, evq, ctx, &shared_res.pgqueue, ncc)
            .instrument(logspan)
            .await
        {
            Ok(ret) => Ok(ret),
            Err(e) => Ok(e.response(ctx.reqid())),
        }
    }
}

async fn plain_events(
    req: Requ,
    evq: PlainEventsQuery,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let ch_conf = chconf_from_events_quorum(&evq, ctx, pgqueue, ncc)
        .await?
        .ok_or_else(|| Error::ChannelNotFound)?;
    if accepts_cbor_framed(req.headers()) {
        Ok(plain_events_cbor_framed(req, evq, ch_conf, ctx, ncc).await?)
    } else if accepts_json_framed(req.headers()) {
        Ok(plain_events_json_framed(req, evq, ch_conf, ctx, ncc).await?)
    } else if accepts_json_or_all(req.headers()) {
        Ok(plain_events_json(req, evq, ch_conf, ctx, ncc).await?)
    } else {
        let ret = error_response(format!("unsupported accept"), ctx.reqid());
        Ok(ret)
    }
}

async fn plain_events_cbor_framed(
    req: Requ,
    evq: PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    debug!("plain_events_cbor_framed  {ch_conf:?}  {req:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let open_bytes = Arc::pin(open_bytes);
    let timeout_provider = streamio::streamtimeout::StreamTimeout::boxed();
    let stream = plain_events_cbor_stream(&evq, ch_conf, ctx, open_bytes, timeout_provider).await?;
    let stream = bytes_chunks_to_framed(stream);
    let logspan = if evq.log_level() == "trace" {
        trace!("enable trace for handler");
        tracing::span!(tracing::Level::INFO, "log_span_trace")
    } else if evq.log_level() == "debug" {
        debug!("enable debug for handler");
        tracing::span!(tracing::Level::INFO, "log_span_debug")
    } else {
        tracing::Span::none()
    };
    let stream = InstrumentStream::new(stream, logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_CBOR_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}

async fn plain_events_json_framed(
    req: Requ,
    evq: PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    debug!("plain_events_json_framed  {ch_conf:?}  {req:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let open_bytes = Arc::pin(open_bytes);
    let timeout_provider = streamio::streamtimeout::StreamTimeout::boxed();
    let stream = plain_events_json_stream(&evq, ch_conf, ctx, open_bytes, timeout_provider).await?;
    let stream = bytes_chunks_to_len_framed_str(stream);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}

async fn plain_events_json(
    req: Requ,
    evq: PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let self_name = "plain_events_json";
    debug!("{self_name}  {ch_conf:?}  {req:?}");
    let (_head, _body) = req.into_parts();
    // TODO handle None case better and return 404
    debug!("{self_name}  chconf_from_events_quorum: {ch_conf:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let open_bytes = Arc::pin(open_bytes);
    let timeout_provider = streamio::streamtimeout::StreamTimeout::boxed();
    let item = streams::plaineventsjson::plain_events_json(
        &evq,
        ch_conf,
        ctx,
        &ncc.node_config.cluster,
        open_bytes,
        timeout_provider,
    )
    .await;
    debug!("{self_name}  returned  {}", item.is_ok());
    let item = match item {
        Ok(item) => item,
        Err(e) => {
            error!("{self_name}  got error from streams::plaineventsjson::plain_events_json {e}");
            return Err(e.into());
        }
    };
    match item {
        CollectResult::Some(item) => {
            let ret = response(StatusCode::OK)
                .header(CONTENT_TYPE, APP_JSON)
                .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
                .body(ToJsonBody::from(&item).into_body())?;
            debug!("{self_name}  response created");
            Ok(ret)
        }
        CollectResult::Timeout => {
            let ret = error_status_response(
                StatusCode::GATEWAY_TIMEOUT,
                format!("no data within timeout"),
                ctx.reqid(),
            );
            Ok(ret)
        }
    }
}
