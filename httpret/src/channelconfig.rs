use crate::err::Error;
use crate::{response, ToPublicResponse};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::{ChannelConfigQuery, FromUrl, ScalarType, Shape};
use netpod::{ChannelConfigResponse, NodeConfigCached};
use netpod::{ACCEPT_ALL, APP_JSON};
use scylla::frame::response::cql_to_rust::FromRowError as ScyFromRowError;
use scylla::transport::errors::{NewSessionError as ScyNewSessionError, QueryError as ScyQueryError};
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

trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T> ErrConv<T> for Result<T, ScyQueryError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, ScyNewSessionError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, ScyFromRowError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

pub async fn channel_config(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("channel_config");
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    //let pairs = get_url_query_pairs(&url);
    let q = ChannelConfigQuery::from_url(&url)?;
    info!("channel_config  for q {q:?}");
    let conf = if q.channel.backend == "scylla" {
        // Find the "series" id.
        let scy = scylla::SessionBuilder::new()
            .known_node("sf-daqbuf-34:8340")
            .build()
            .await
            .err_conv()?;
        let cql = "select dtype, series from series_by_channel where facility = ? and channel_name = ?";
        let res = scy.query(cql, ()).await.err_conv()?;
        let rows = res.rows_typed_or_empty::<(i32, i32)>();
        for r in rows {
            let r = r.err_conv()?;
            info!("got row  {r:?}");
        }
        let res = ChannelConfigResponse {
            channel: q.channel,
            scalar_type: ScalarType::F32,
            byte_order: None,
            shape: Shape::Scalar,
        };
        res
    } else if let Some(conf) = &node_config.node.channel_archiver {
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
