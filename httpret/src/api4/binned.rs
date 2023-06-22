use crate::bodystream::response;
use crate::bodystream::ToPublicResponse;
use crate::channelconfig::ch_conf_from_binned;
use crate::err::Error;
use crate::response_err;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use netpod::APP_OCTET;
use query::api4::binned::BinnedQuery;
use tracing::Instrument;
use url::Url;

async fn binned_json(url: Url, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    debug!("httpret  plain_events_json  req: {:?}", req);
    let (_head, _body) = req.into_parts();
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        error!("binned_json: {e:?}");
        let msg = format!("can not parse query: {}", e.msg());
        e.add_public_msg(msg)
    })?;
    let ch_conf = ch_conf_from_binned(&query, node_config).await?;
    let span1 = span!(
        Level::INFO,
        "httpret::binned",
        beg = query.range().beg_u64() / SEC,
        end = query.range().end_u64() / SEC,
        ch = query.channel().name().clone(),
    );
    span1.in_scope(|| {
        debug!("begin");
    });
    let item = streams::timebinnedjson::timebinned_json(query, ch_conf, node_config.node_config.cluster.clone())
        .instrument(span1)
        .await?;
    let buf = serde_json::to_vec(&item)?;
    let ret = response(StatusCode::OK).body(Body::from(buf))?;
    Ok(ret)
}

async fn binned(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let accept = req
        .headers()
        .get(http::header::ACCEPT)
        .map_or(ACCEPT_ALL, |k| k.to_str().unwrap_or(ACCEPT_ALL));
    let url = {
        let s1 = format!("dummy:{}", req.uri());
        Url::parse(&s1)
            .map_err(Error::from)
            .map_err(|e| e.add_public_msg(format!("Can not parse query url")))?
    };
    if req
        .uri()
        .path_and_query()
        .map_or(false, |x| x.as_str().contains("DOERR"))
    {
        Err(Error::with_msg_no_trace("hidden message").add_public_msg("PublicMessage"))?;
    }
    if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
        Ok(binned_json(url, req, node_config).await?)
    } else if accept == APP_OCTET {
        Ok(response_err(
            StatusCode::NOT_ACCEPTABLE,
            format!("binary binned data not yet available"),
        )?)
    } else {
        let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("Unsupported Accept: {:?}", accept))?;
        Ok(ret)
    }
}

pub struct BinnedHandler {}

impl BinnedHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/binned" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        match binned(req, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => {
                warn!("BinnedHandler handle sees: {e}");
                Ok(e.to_public_response())
            }
        }
    }
}
