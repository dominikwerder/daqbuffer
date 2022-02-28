use crate::err::Error;
use crate::{response, ToPublicResponse};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::{ChannelConfigQuery, FromUrl};
use netpod::{ACCEPT_ALL, APP_JSON};
use url::Url;

pub struct ChannelConfigHandler {}

impl ChannelConfigHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/channel/config" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                match channel_config(req, &node_config).await {
                    Ok(k) => Ok(k),
                    Err(e) => {
                        warn!("ChannelConfigHandler::handle: got error from channel_config: {e:?}");
                        Ok(e.to_public_response())
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}

pub async fn channel_config(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    //let pairs = get_url_query_pairs(&url);
    let q = ChannelConfigQuery::from_url(&url)?;
    let conf = if let Some(conf) = &node_config.node.channel_archiver {
        archapp_wrap::archapp::archeng::channel_config_from_db(&q, conf, &node_config.node_config.cluster.database)
            .await?
    } else if let Some(conf) = &node_config.node.archiver_appliance {
        archapp_wrap::channel_config(&q, conf).await?
    } else {
        parse::channelconfig::channel_config(&q, &node_config.node).await?
    };
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON)
        .body(Body::from(serde_json::to_string(&conf)?))?;
    Ok(ret)
}
