use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use futures_util::StreamExt;
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use items_2::channelevents::ConnStatusEvent;
use netpod::query::ChannelStateEventsQuery;
use netpod::{FromUrl, NodeConfigCached, ACCEPT_ALL, APP_JSON};
use url::Url;

pub struct ConnectionStatusEvents {}

impl ConnectionStatusEvents {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/connection/status/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Request<Body>,
        _ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<Response<Body>, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                let url = Url::parse(&format!("dummy:{}", req.uri()))?;
                let q = ChannelStateEventsQuery::from_url(&url)?;
                match self.fetch_data(&q, node_config).await {
                    Ok(k) => {
                        let body = Body::from(serde_json::to_vec(&k)?);
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => Ok(response(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(format!("{:?}", e.public_msg())))?),
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn fetch_data(
        &self,
        q: &ChannelStateEventsQuery,
        node_config: &NodeConfigCached,
    ) -> Result<Vec<ConnStatusEvent>, Error> {
        let scyco = node_config
            .node_config
            .cluster
            .scylla
            .as_ref()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("No Scylla configured")))?;
        let scy = scyllaconn::create_scy_session(scyco).await?;
        let mut stream = scyllaconn::events::channel_state_events(q, scy).await?;
        let mut ret = Vec::new();
        while let Some(item) = stream.next().await {
            let item = item?;
            ret.push(item);
        }
        Ok(ret)
    }
}

pub struct ChannelStatusEvents {}

impl ChannelStatusEvents {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/channel/status/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Request<Body>,
        _ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<Response<Body>, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                let url = Url::parse(&format!("dummy:{}", req.uri()))?;
                let q = ChannelStateEventsQuery::from_url(&url)?;
                match self.fetch_data(&q, node_config).await {
                    Ok(k) => {
                        let body = Body::from(serde_json::to_vec(&k)?);
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => Ok(response(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(format!("{:?}", e.public_msg())))?),
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn fetch_data(
        &self,
        q: &ChannelStateEventsQuery,
        node_config: &NodeConfigCached,
    ) -> Result<Vec<ConnStatusEvent>, Error> {
        let scyco = node_config
            .node_config
            .cluster
            .scylla
            .as_ref()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("No Scylla configured")))?;
        let scy = scyllaconn::create_scy_session(scyco).await?;
        let mut stream = scyllaconn::events::channel_state_events(q, scy).await?;
        let mut ret = Vec::new();
        while let Some(item) = stream.next().await {
            let item = item?;
            ret.push(item);
        }
        Ok(ret)
    }
}
