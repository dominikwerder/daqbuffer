use crate::bodystream::response_err_msg;
use crate::channelconfig::chconf_from_events_quorum;
use crate::err::Error;
use crate::requests::accepts_cbor_framed;
use crate::requests::accepts_json_framed;
use crate::requests::accepts_json_or_all;
use crate::response;
use crate::ServiceSharedResources;
use crate::ToPublicResponse;
use bytes::Bytes;
use bytes::BytesMut;
use dbconn::worker::PgQueue;
use futures_util::future;
use futures_util::stream;
use futures_util::Stream;
use futures_util::StreamExt;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use nodenet::client::OpenBoxedBytesViaHttp;
use query::api4::events::PlainEventsQuery;
use streams::instrument::InstrumentStream;
use tracing::Instrument;

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
    ) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let self_name = "handle";
        let url = req_uri_to_url(req.uri())?;
        let evq =
            PlainEventsQuery::from_url(&url).map_err(|e| e.add_public_msg(format!("Can not understand query")))?;
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
            Err(e) => {
                error!("EventsHandler sees: {e}");
                Ok(e.to_public_response())
            }
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
    if accepts_cbor_framed(req.headers()) {
        Ok(plain_events_cbor_framed(req, evq, ctx, pgqueue, ncc).await?)
    } else if accepts_json_framed(req.headers()) {
        Ok(plain_events_json_framed(req, evq, ctx, pgqueue, ncc).await?)
    } else if accepts_json_or_all(req.headers()) {
        Ok(plain_events_json(req, evq, ctx, pgqueue, ncc).await?)
    } else {
        let ret = response_err_msg(StatusCode::NOT_ACCEPTABLE, format!("unsupported accept  {:?}", req))?;
        Ok(ret)
    }
}

async fn plain_events_cbor_framed(
    req: Requ,
    evq: PlainEventsQuery,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let ch_conf = chconf_from_events_quorum(&evq, ctx, pgqueue, ncc)
        .await?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
    debug!("plain_events_cbor_framed  chconf_from_events_quorum: {ch_conf:?}  {req:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let stream = streams::plaineventscbor::plain_events_cbor_stream(&evq, ch_conf, ctx, Box::pin(open_bytes)).await?;
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
    let ret = response(StatusCode::OK).body(body_stream(stream))?;
    Ok(ret)
}

async fn plain_events_json_framed(
    req: Requ,
    evq: PlainEventsQuery,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let ch_conf = chconf_from_events_quorum(&evq, ctx, pgqueue, ncc)
        .await?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
    debug!("plain_events_json_framed  chconf_from_events_quorum: {ch_conf:?}  {req:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let stream = streams::plaineventsjson::plain_events_json_stream(&evq, ch_conf, ctx, Box::pin(open_bytes)).await?;
    let stream = bytes_chunks_to_len_framed_str(stream);
    let ret = response(StatusCode::OK).body(body_stream(stream))?;
    Ok(ret)
}

async fn plain_events_json(
    req: Requ,
    evq: PlainEventsQuery,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let self_name = "plain_events_json";
    debug!("{self_name}  req: {:?}", req);
    let (_head, _body) = req.into_parts();
    // TODO handle None case better and return 404
    let ch_conf = chconf_from_events_quorum(&evq, ctx, pgqueue, ncc)
        .await
        .map_err(Error::from)?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
    debug!("{self_name}  chconf_from_events_quorum: {ch_conf:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let item =
        streams::plaineventsjson::plain_events_json(&evq, ch_conf, ctx, &ncc.node_config.cluster, Box::pin(open_bytes))
            .await;
    debug!("{self_name}  returned  {}", item.is_ok());
    let item = match item {
        Ok(item) => item,
        Err(e) => {
            error!("{self_name}  got error from streams::plaineventsjson::plain_events_json {e}");
            return Err(e.into());
        }
    };
    let ret = response(StatusCode::OK).body(ToJsonBody::from(&item).into_body())?;
    debug!("{self_name}  response created");
    Ok(ret)
}

fn bytes_chunks_to_framed<S, T>(stream: S) -> impl Stream<Item = Result<Bytes, Error>>
where
    S: Stream<Item = Result<T, err::Error>>,
    T: Into<Bytes>,
{
    use future::ready;
    stream
        // TODO unify this map to padded bytes for both json and cbor output
        .flat_map(|x| match x {
            Ok(y) => {
                use bytes::BufMut;
                let buf = y.into();
                let adv = (buf.len() + 7) / 8 * 8;
                let pad = adv - buf.len();
                let mut b2 = BytesMut::with_capacity(16);
                b2.put_u32_le(buf.len() as u32);
                b2.put_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
                let mut b3 = BytesMut::with_capacity(16);
                b3.put_slice(&[0, 0, 0, 0, 0, 0, 0, 0][..pad]);
                stream::iter([Ok::<_, Error>(b2.freeze()), Ok(buf), Ok(b3.freeze())])
            }
            Err(e) => {
                let e = Error::with_msg_no_trace(e.to_string());
                stream::iter([Err(e), Ok(Bytes::new()), Ok(Bytes::new())])
            }
        })
        .filter(|x| if let Ok(x) = x { ready(x.len() > 0) } else { ready(true) })
}

fn bytes_chunks_to_len_framed_str<S, T>(stream: S) -> impl Stream<Item = Result<String, Error>>
where
    S: Stream<Item = Result<T, err::Error>>,
    T: Into<String>,
{
    use future::ready;
    stream
        .flat_map(|x| match x {
            Ok(y) => {
                use std::fmt::Write;
                let s = y.into();
                let mut b2 = String::with_capacity(16);
                write!(b2, "\n{}\n", s.len()).unwrap();
                stream::iter([Ok::<_, Error>(b2), Ok(s)])
            }
            Err(e) => {
                let e = Error::with_msg_no_trace(e.to_string());
                stream::iter([Err(e), Ok(String::new())])
            }
        })
        .filter(|x| if let Ok(x) = x { ready(x.len() > 0) } else { ready(true) })
}
