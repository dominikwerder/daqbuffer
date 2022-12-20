use crate::bodystream::{response, ToPublicResponse};
use crate::channelconfig::chconf_from_binned;
use crate::err::Error;
use crate::response_err;
use http::{Method, StatusCode};
use http::{Request, Response};
use hyper::Body;
use netpod::log::*;
use netpod::query::BinnedQuery;
use netpod::timeunits::SEC;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::{ACCEPT_ALL, APP_JSON, APP_OCTET};
use tracing::Instrument;
use url::Url;

async fn binned_json(url: Url, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    debug!("httpret  plain_events_json  req: {:?}", req);
    let (_head, _body) = req.into_parts();
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        let msg = format!("can not parse query: {}", e.msg());
        e.add_public_msg(msg)
    })?;
    let chconf = chconf_from_binned(&query, node_config).await?;
    // Update the series id since we don't require some unique identifier yet.
    let mut query = query;
    query.set_series_id(chconf.series);
    let query = query;
    // ---
    let span1 = span!(
        Level::INFO,
        "httpret::binned",
        beg = query.range().beg / SEC,
        end = query.range().end / SEC,
        ch = query.channel().name(),
    );
    span1.in_scope(|| {
        debug!("begin");
    });
    let item = streams::timebinnedjson::timebinned_json(&query, &chconf, &node_config.node_config.cluster)
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
            Err(e) => Ok(e.to_public_response()),
        }
    }
}
