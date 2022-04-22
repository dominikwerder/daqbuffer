use crate::err::Error;
use crate::{response, response_err, BodyStream, ToPublicResponse};
use disk::events::{PlainEventsBinaryQuery, PlainEventsJsonQuery};
use futures_util::{StreamExt, TryStreamExt};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::{AggKind, NodeConfigCached};
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
            Err(e) => Ok(e.to_public_response()),
        }
    }
}

async fn plain_events(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("httpret  plain_events  req: {:?}", req);
    let accept_def = APP_JSON;
    let accept = req
        .headers()
        .get(http::header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
    if accept == APP_JSON || accept == ACCEPT_ALL {
        Ok(plain_events_json(req, node_config).await?)
    } else if accept == APP_OCTET {
        Ok(plain_events_binary(req, node_config).await?)
    } else {
        let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("Unsupported Accept: {:?}", accept))?;
        Ok(ret)
    }
}

async fn plain_events_binary(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    debug!("httpret  plain_events_binary  req: {:?}", req);
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    let query = PlainEventsBinaryQuery::from_url(&url)?;
    let op = disk::channelexec::PlainEvents::new(
        query.channel().clone(),
        query.range().clone(),
        query.disk_io_buffer_size(),
        node_config.clone(),
    );
    let s = disk::channelexec::channel_exec(op, query.channel(), query.range(), AggKind::Plain, node_config).await?;
    let s = s.map(|item| item.make_frame());
    let ret = response(StatusCode::OK).body(BodyStream::wrapped(
        s.map_err(Error::from),
        format!("plain_events_binary"),
    ))?;
    Ok(ret)
}

async fn plain_events_json(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("httpret  plain_events_json  req: {:?}", req);
    let (head, _body) = req.into_parts();
    let query = PlainEventsJsonQuery::from_request_head(&head)?;
    let op = disk::channelexec::PlainEventsJson::new(
        query.channel().clone(),
        query.range().clone(),
        query.disk_io_buffer_size(),
        query.timeout(),
        node_config.clone(),
        query.events_max().unwrap_or(u64::MAX),
        query.do_log(),
    );
    let s = disk::channelexec::channel_exec(op, query.channel(), query.range(), AggKind::Plain, node_config).await?;
    let ret = response(StatusCode::OK).body(BodyStream::wrapped(
        s.map_err(Error::from),
        format!("plain_events_json"),
    ))?;
    Ok(ret)
}
