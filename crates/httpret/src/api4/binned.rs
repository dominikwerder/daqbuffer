use crate::bodystream::response;
use crate::channelconfig::ch_conf_from_binned;
use crate::requests::accepts_json_or_all;
use crate::requests::accepts_octets;
use crate::ServiceSharedResources;
use dbconn::worker::PgQueue;
use err::thiserror;
use err::ThisError;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::error_response;
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
use nodenet::client::OpenBoxedBytesViaHttp;
use query::api4::binned::BinnedQuery;
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
    BinnedStream(::err::Error),
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
        match binned(req, ctx, &shared_res.pgqueue, ncc).await {
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

async fn binned(req: Requ, ctx: &ReqCtx, pgqueue: &PgQueue, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
    let url = req_uri_to_url(req.uri()).map_err(|e| Error::BadQuery(e.to_string()))?;
    if req
        .uri()
        .path_and_query()
        .map_or(false, |x| x.as_str().contains("DOERR"))
    {
        Err(Error::ServerError)?;
    }
    if accepts_json_or_all(&req.headers()) {
        Ok(binned_json(url, req, ctx, pgqueue, ncc).await?)
    } else if accepts_octets(&req.headers()) {
        Ok(error_response(
            format!("binary binned data not yet available"),
            ctx.reqid(),
        ))
    } else {
        let ret = error_response(format!("Unsupported Accept: {:?}", req.headers()), ctx.reqid());
        Ok(ret)
    }
}

async fn binned_json(
    url: Url,
    req: Requ,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    debug!("{:?}", req);
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
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let open_bytes = Box::pin(open_bytes);
    let item = streams::timebinnedjson::timebinned_json(query, ch_conf, ctx, open_bytes)
        .instrument(span1)
        .await
        .map_err(|e| Error::BinnedStream(e))?;
    let ret = response(StatusCode::OK).body(ToJsonBody::from(&item).into_body())?;
    // let ret = error_response(e.public_message(), ctx.reqid());
    Ok(ret)
}
