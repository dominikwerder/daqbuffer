use crate::channelconfig::{chconf_from_events_binary, chconf_from_events_json};
use crate::err::Error;
use crate::{response, response_err, BodyStream, ToPublicResponse};
use futures_util::{stream, TryStreamExt};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::{ACCEPT_ALL, APP_JSON, APP_OCTET};
use url::Url;

pub struct EventsHandler {}

impl EventsHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        match plain_events(req, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => {
                error!("EventsHandler sees {e}");
                Ok(e.to_public_response())
            }
        }
    }
}

async fn plain_events(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let accept_def = APP_JSON;
    let accept = req
        .headers()
        .get(http::header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
    let url = {
        let s1 = format!("dummy:{}", req.uri());
        Url::parse(&s1)
            .map_err(Error::from)
            .map_err(|e| e.add_public_msg(format!("Can not parse query url")))?
    };
    if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
        Ok(plain_events_json(url, req, node_config).await?)
    } else if accept == APP_OCTET {
        Ok(plain_events_binary(url, req, node_config).await?)
    } else {
        let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("Unsupported Accept: {:?}", accept))?;
        Ok(ret)
    }
}

async fn plain_events_binary(
    url: Url,
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
    debug!("httpret  plain_events_binary  req: {:?}", req);
    let query = PlainEventsQuery::from_url(&url).map_err(|e| e.add_public_msg(format!("Can not understand query")))?;
    let chconf = chconf_from_events_binary(&query, node_config).await?;
    // Update the series id since we don't require some unique identifier yet.
    let mut query = query;
    query.set_series_id(chconf.series);
    let query = query;
    // ---
    let _ = query;
    let s = stream::iter([Ok::<_, Error>(String::from("TODO_PREBINNED_BINARY_STREAM"))]);
    let ret = response(StatusCode::OK).body(BodyStream::wrapped(
        s.map_err(Error::from),
        format!("plain_events_binary"),
    ))?;
    Ok(ret)
}

async fn plain_events_json(
    url: Url,
    req: Request<Body>,
    node_config: &NodeConfigCached,
) -> Result<Response<Body>, Error> {
    info!("httpret  plain_events_json  req: {:?}", req);
    let (_head, _body) = req.into_parts();
    let query = PlainEventsQuery::from_url(&url)?;
    info!("plain_events_json query {query:?}");
    let chconf = chconf_from_events_json(&query, node_config)
        .await
        .map_err(Error::from)?;
    // Update the series id since we don't require some unique identifier yet.
    let mut query = query;
    query.set_series_id(chconf.series);
    let query = query;
    // ---
    //let query = RawEventsQuery::new(query.channel().clone(), query.range().clone(), AggKind::Plain);
    let item = streams::plaineventsjson::plain_events_json(&query, &chconf, &node_config.node_config.cluster).await;
    let item = match item {
        Ok(item) => item,
        Err(e) => {
            error!("got error from streams::plaineventsjson::plain_events_json {e:?}");
            return Err(e.into());
        }
    };
    let buf = serde_json::to_vec(&item)?;
    let ret = response(StatusCode::OK).body(Body::from(buf))?;
    Ok(ret)
}
