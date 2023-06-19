use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use futures_util::StreamExt;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use items_2::channelevents::ChannelStatusEvent;
use items_2::channelevents::ConnStatusEvent;
use netpod::log::*;
use netpod::query::ChannelStateEventsQuery;
use netpod::ChannelTypeConfigGen;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use url::Url;

pub struct ConnectionStatusEvents {}

impl ConnectionStatusEvents {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/status/connection/events" {
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
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                let url = Url::parse(&format!("dummy:{}", req.uri()))?;
                let q = ChannelStateEventsQuery::from_url(&url)?;
                match self.fetch_data(&q, node_config).await {
                    Ok(k) => {
                        let body = Body::from(serde_json::to_vec(&k)?);
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => {
                        error!("{e}");
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from(format!("{:?}", e.public_msg())))?)
                    }
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
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("no scylla configured")))?;
        let _scy = scyllaconn::create_scy_session(scyco).await?;
        let chconf =
            nodenet::channelconfig::channel_config(q.range().clone(), q.channel().clone(), node_config).await?;
        let _do_one_before_range = true;
        let ret = Vec::new();
        if true {
            return Err(Error::with_msg_no_trace("TODO channel_status fetch_data"));
        }
        /*let mut stream =
            scyllaconn::status::StatusStreamScylla::new(series, q.range().clone(), do_one_before_range, scy);
        while let Some(item) = stream.next().await {
            let item = item?;
            ret.push(item);
        }*/
        Ok(ret)
    }
}

pub struct ChannelStatusEvents {}

impl ChannelStatusEvents {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/status/channel/events" {
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
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                let url = Url::parse(&format!("dummy:{}", req.uri()))?;
                let q = ChannelStateEventsQuery::from_url(&url)?;
                match self.fetch_data(&q, node_config).await {
                    Ok(k) => {
                        let body = Body::from(serde_json::to_vec(&k)?);
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => {
                        error!("{e}");
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from(format!("{:?}", e.public_msg())))?)
                    }
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
    ) -> Result<Vec<ChannelStatusEvent>, Error> {
        let scyco = node_config
            .node_config
            .cluster
            .scylla
            .as_ref()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("no scylla configured")))?;
        let scy = scyllaconn::create_scy_session(scyco).await?;
        let chconf =
            nodenet::channelconfig::channel_config(q.range().clone(), q.channel().clone(), node_config).await?;
        let do_one_before_range = true;
        match chconf {
            ChannelTypeConfigGen::Scylla(ch_conf) => {
                let mut stream = scyllaconn::status::StatusStreamScylla::new(
                    ch_conf.series(),
                    q.range().clone(),
                    do_one_before_range,
                    scy,
                );
                let mut ret = Vec::new();
                while let Some(item) = stream.next().await {
                    let item = item?;
                    ret.push(item);
                }
                Ok(ret)
            }
            ChannelTypeConfigGen::SfDatabuffer(k) => todo!(),
        }
    }
}
