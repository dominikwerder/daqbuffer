use crate::bodystream::response;
use crate::bodystream::response_err_msg;
use crate::bodystream::ToPublicResponse;
use crate::channelconfig::ch_conf_from_binned;
use crate::err::Error;
use crate::requests::accepts_json_or_all;
use crate::requests::accepts_octets;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
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

async fn binned_json(url: Url, req: Requ, ctx: &ReqCtx, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
    debug!("{:?}", req);
    let reqid = crate::status_board()
        .map_err(|e| Error::with_msg_no_trace(e.to_string()))?
        .new_status_id();
    let (_head, _body) = req.into_parts();
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        error!("binned_json: {e:?}");
        let msg = format!("can not parse query: {}", e.msg());
        e.add_public_msg(msg)
    })?;
    // TODO handle None case better and return 404
    let ch_conf = ch_conf_from_binned(&query, ctx, ncc)
        .await?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
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
        .await?;
    let ret = response(StatusCode::OK).body(ToJsonBody::from(&item).into_body())?;
    Ok(ret)
}

async fn binned(req: Requ, ctx: &ReqCtx, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
    let url = req_uri_to_url(req.uri())?;
    if req
        .uri()
        .path_and_query()
        .map_or(false, |x| x.as_str().contains("DOERR"))
    {
        Err(Error::with_msg_no_trace("hidden message").add_public_msg("PublicMessage"))?;
    }
    if accepts_json_or_all(&req.headers()) {
        Ok(binned_json(url, req, ctx, node_config).await?)
    } else if accepts_octets(&req.headers()) {
        Ok(response_err_msg(
            StatusCode::NOT_ACCEPTABLE,
            format!("binary binned data not yet available"),
        )?)
    } else {
        let ret = response_err_msg(
            StatusCode::NOT_ACCEPTABLE,
            format!("Unsupported Accept: {:?}", req.headers()),
        )?;
        Ok(ret)
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
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        match binned(req, ctx, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => {
                warn!("BinnedHandler handle sees: {e}");
                Ok(e.to_public_response())
            }
        }
    }
}
