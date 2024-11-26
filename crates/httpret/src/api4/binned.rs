use crate::bodystream::response;
use crate::channelconfig::ch_conf_from_binned;
use crate::requests::accepts_json_framed;
use crate::requests::accepts_json_or_all;
use crate::requests::accepts_octets;
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
use httpclient::not_found_response;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::timeunits::SEC;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::APP_JSON;
use netpod::APP_JSON_FRAMED;
use netpod::HEADER_NAME_REQUEST_ID;
use nodenet::client::OpenBoxedBytesViaHttp;
use nodenet::scylla::ScyllaEventReadProvider;
use query::api4::binned::BinnedQuery;
use scyllaconn::bincache::ScyllaCacheReadProvider;
use scyllaconn::worker::ScyllaQueue;
use std::pin::Pin;
use std::sync::Arc;
use streams::collect::CollectResult;
use streams::eventsplainreader::DummyCacheReadProvider;
use streams::eventsplainreader::SfDatabufferEventReadProvider;
use streams::lenframe::bytes_chunks_to_len_framed_str;
use streams::timebin::cached::reader::EventsReadProvider;
use streams::timebin::CacheReadProvider;
use tracing::Instrument;
use url::Url;

#[derive(Debug, ThisError)]
#[cstm(name = "Api4Binned")]
pub enum Error {
    ChannelNotFound,
    BadQuery(String),
    HttpLib(#[from] http::Error),
    ChannelConfig(crate::channelconfig::Error),
    Retrieval(#[from] crate::RetrievalError),
    EventsCbor(#[from] streams::plaineventscbor::Error),
    EventsJson(#[from] streams::plaineventsjson::Error),
    ServerError,
    BinnedStream(err::Error),
    TimebinnedJson(#[from] streams::timebinnedjson::Error),
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

pub struct BinnedHandler {}

impl BinnedHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/binned" {
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
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?);
        }
        match binned(req, ctx, &shared_res.pgqueue, shared_res.scyqueue.clone(), ncc).await {
            Ok(ret) => Ok(ret),
            Err(e) => match e {
                Error::ChannelNotFound => {
                    let res = not_found_response("channel not found".into(), ctx.reqid());
                    Ok(res)
                }
                Error::BadQuery(msg) => {
                    let res = error_response(format!("bad query: {msg}"), ctx.reqid());
                    Ok(res)
                }
                _ => {
                    error!("EventsHandler sees: {e}");
                    Ok(error_response(e.public_message(), ctx.reqid()))
                }
            },
        }
    }
}

async fn binned(
    req: Requ,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    scyqueue: Option<ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let url = req_uri_to_url(req.uri()).map_err(|e| Error::BadQuery(e.to_string()))?;
    if req
        .uri()
        .path_and_query()
        .map_or(false, |x| x.as_str().contains("DOERR"))
    {
        Err(Error::ServerError)?;
    }
    if accepts_json_framed(req.headers()) {
        Ok(binned_json_framed(url, req, ctx, pgqueue, scyqueue, ncc).await?)
    } else if accepts_json_or_all(req.headers()) {
        Ok(binned_json_single(url, req, ctx, pgqueue, scyqueue, ncc).await?)
    } else if accepts_octets(req.headers()) {
        Ok(error_response(
            format!("binary binned data not yet available"),
            ctx.reqid(),
        ))
    } else {
        let ret = error_response(format!("Unsupported Accept: {:?}", req.headers()), ctx.reqid());
        Ok(ret)
    }
}

fn make_read_provider(
    chname: &str,
    scyqueue: Option<ScyllaQueue>,
    open_bytes: Pin<Arc<OpenBoxedBytesViaHttp>>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> (Arc<dyn EventsReadProvider>, Arc<dyn CacheReadProvider>) {
    let events_read_provider = if chname.starts_with("unittest") {
        let x = streams::teststream::UnitTestStream::new();
        Arc::new(x)
    } else if ncc.node_config.cluster.scylla_lt().is_some() {
        scyqueue
            .clone()
            .map(|qu| ScyllaEventReadProvider::new(qu))
            .map(|x| Arc::new(x) as Arc<dyn EventsReadProvider>)
            .expect("expect scylla queue")
    } else if ncc.node.sf_databuffer.is_some() {
        // TODO do not clone the request. Pass an Arc up to here.
        let x = SfDatabufferEventReadProvider::new(Arc::new(ctx.clone()), open_bytes);
        Arc::new(x)
    } else {
        panic!()
    };
    let cache_read_provider = if ncc.node_config.cluster.scylla_lt().is_some() {
        scyqueue
            .clone()
            .map(|qu| ScyllaCacheReadProvider::new(qu))
            .map(|x| Arc::new(x) as Arc<dyn CacheReadProvider>)
            .expect("expect scylla queue")
    } else if ncc.node.sf_databuffer.is_some() {
        let x = DummyCacheReadProvider::new();
        Arc::new(x)
    } else {
        panic!()
    };
    (events_read_provider, cache_read_provider)
}

async fn binned_json_single(
    url: Url,
    req: Requ,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    scyqueue: Option<ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    // TODO unify with binned_json_framed
    debug!("binned_json_single  {:?}", req);
    let reqid = crate::status_board().map_err(|_e| Error::ServerError)?.new_status_id();
    let (_head, _body) = req.into_parts();
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        error!("binned_json: {e:?}");
        Error::BadQuery(e.to_string())
    })?;
    let ch_conf = ch_conf_from_binned(&query, ctx, pgqueue, ncc)
        .await?
        .ok_or_else(|| Error::ChannelNotFound)?;
    let span1 = span!(
        Level::INFO,
        "httpret::binned",
        reqid,
        beg = query.range().beg_u64() / SEC,
        end = query.range().end_u64() / SEC,
        ch = query.channel().name(),
    );
    span1.in_scope(|| {
        debug!("begin");
    });
    let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
    let (events_read_provider, cache_read_provider) =
        make_read_provider(ch_conf.name(), scyqueue, open_bytes, ctx, ncc);
    let timeout_provider = streamio::streamtimeout::StreamTimeout::boxed();
    let item = streams::timebinnedjson::timebinned_json(
        query,
        ch_conf,
        ctx,
        cache_read_provider,
        events_read_provider,
        timeout_provider,
    )
    .instrument(span1)
    .await?;
    match item {
        CollectResult::Some(item) => {
            let ret = response(StatusCode::OK)
                .header(CONTENT_TYPE, APP_JSON)
                .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
                .body(ToJsonBody::from(&item).into_body())?;
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

async fn binned_json_framed(
    url: Url,
    req: Requ,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    scyqueue: Option<ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    debug!("binned_json_framed  {:?}", req);
    let reqid = crate::status_board().map_err(|_e| Error::ServerError)?.new_status_id();
    let (_head, _body) = req.into_parts();
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        error!("binned_json: {e:?}");
        Error::BadQuery(e.to_string())
    })?;
    // TODO handle None case better and return 404
    let ch_conf = ch_conf_from_binned(&query, ctx, pgqueue, ncc)
        .await?
        .ok_or_else(|| Error::ChannelNotFound)?;
    let span1 = span!(
        Level::INFO,
        "httpret::binned",
        reqid,
        beg = query.range().beg_u64() / SEC,
        end = query.range().end_u64() / SEC,
        ch = query.channel().name(),
    );
    span1.in_scope(|| {
        debug!("begin");
    });
    let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
    let (events_read_provider, cache_read_provider) =
        make_read_provider(ch_conf.name(), scyqueue, open_bytes, ctx, ncc);
    let timeout_provider = streamio::streamtimeout::StreamTimeout::boxed();
    let stream = streams::timebinnedjson::timebinned_json_framed(
        query,
        ch_conf,
        ctx,
        cache_read_provider,
        events_read_provider,
        timeout_provider,
    )
    .instrument(span1)
    .await?;
    let stream = bytes_chunks_to_len_framed_str(stream);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}
