use std::collections::BTreeMap;

use crate::err::Error;
use crate::{response, ToPublicResponse};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::{get_url_query_pairs, Channel, ChannelConfigQuery, FromUrl, ScalarType, ScyllaConfig, Shape};
use netpod::{ChannelConfigResponse, NodeConfigCached};
use netpod::{ACCEPT_ALL, APP_JSON};
use scylla::batch::Consistency;
use scylla::frame::response::cql_to_rust::FromRowError as ScyFromRowError;
use scylla::transport::errors::{NewSessionError as ScyNewSessionError, QueryError as ScyQueryError};
use serde::{Deserialize, Serialize};
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

async fn config_from_scylla(
    chq: ChannelConfigQuery,
    scyco: &ScyllaConfig,
    _node_config: &NodeConfigCached,
) -> Result<ChannelConfigResponse, Error> {
    // Find the "series" id.
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyco.hosts)
        .use_keyspace(&scyco.keyspace, true)
        .default_consistency(Consistency::One)
        .build()
        .await
        .err_conv()?;
    let cql = "select series, scalar_type, shape_dims from series_by_channel where facility = ? and channel_name = ?";
    let res = scy
        .query(cql, (&chq.channel.backend, chq.channel.name()))
        .await
        .err_conv()?;
    let rows: Vec<_> = res.rows_typed_or_empty::<(i64, i32, Vec<i32>)>().collect();
    if rows.len() == 0 {
        return Err(Error::with_public_msg_no_trace(format!(
            "can not find series for channel {}",
            chq.channel.name()
        )));
    } else {
        for r in &rows {
            if let Err(e) = r {
                return Err(Error::with_msg_no_trace(format!("error {e:?}")));
            }
            info!("got row  {r:?}");
        }
        let row = rows[0].as_ref().unwrap();
        let scalar_type = ScalarType::from_scylla_i32(row.1)?;
        let shape = Shape::from_scylla_shape_dims(&row.2)?;
        let res = ChannelConfigResponse {
            channel: chq.channel,
            scalar_type,
            byte_order: None,
            shape,
        };
        info!("MADE: {res:?}");
        Ok(res)
    }
}

pub async fn channel_config(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    info!("channel_config");
    let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    let q = ChannelConfigQuery::from_url(&url)?;
    info!("channel_config  for q {q:?}");
    let conf = if q.channel.backend == "scylla" {
        let scyco = node_config
            .node_config
            .cluster
            .scylla
            .as_ref()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("No Scylla configured")))?;
        config_from_scylla(q, scyco, node_config).await?
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigsHisto {
    scalar_types: Vec<(ScalarType, Vec<(Shape, u32)>)>,
}

pub struct ScyllaConfigsHisto {}

impl ScyllaConfigsHisto {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/configs/histo" {
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
                let res = self.make_histo(node_config).await?;
                let body = Body::from(serde_json::to_vec(&res)?);
                Ok(response(StatusCode::OK).body(body)?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn make_histo(&self, node_config: &NodeConfigCached) -> Result<ConfigsHisto, Error> {
        let scyco = node_config
            .node_config
            .cluster
            .scylla
            .as_ref()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("No Scylla configured")))?;
        let scy = scylla::SessionBuilder::new()
            .known_nodes(&scyco.hosts)
            .use_keyspace(&scyco.keyspace, true)
            .default_consistency(Consistency::One)
            .build()
            .await
            .err_conv()?;
        let facility = "scylla";
        let res = scy
            .query(
                "select scalar_type, shape_dims, series from series_by_channel where facility = ? allow filtering",
                (facility,),
            )
            .await
            .err_conv()?;
        let mut stm = BTreeMap::new();
        for row in res.rows_typed_or_empty::<(i32, Vec<i32>, i64)>() {
            let (st, dims, _) = row.err_conv()?;
            let scalar_type = ScalarType::from_scylla_i32(st)?;
            let shape = Shape::from_scylla_shape_dims(&dims)?;
            if stm.get_mut(&scalar_type).is_none() {
                stm.insert(scalar_type.clone(), BTreeMap::new());
            }
            let a = stm.get_mut(&scalar_type).unwrap();
            if a.get_mut(&shape).is_none() {
                a.insert(shape.clone(), 0);
            }
            *a.get_mut(&shape).unwrap() += 1;
        }
        let mut stm: Vec<_> = stm
            .into_iter()
            .map(|(st, m2)| {
                let mut g: Vec<_> = m2.into_iter().map(|(sh, c)| (sh, c)).collect();
                g.sort_by_key(|x| !x.1);
                let n = g.len() as u32;
                (st, g, n)
            })
            .collect();
        stm.sort_unstable_by_key(|x| !x.2);
        let stm = stm.into_iter().map(|(st, a, _)| (st, a)).collect();
        let ret = ConfigsHisto { scalar_types: stm };
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelsWithTypeQuery {
    scalar_type: ScalarType,
    shape: Shape,
}

impl FromUrl for ChannelsWithTypeQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        let s = pairs
            .get("scalar_type")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing scalar_type"))?;
        //let scalar_type = ScalarType::from_bsread_str(s)?;
        let scalar_type: ScalarType = serde_json::from_str(&format!("\"{s}\""))?;
        let s = pairs
            .get("shape")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing shape"))?;
        let shape = Shape::from_dims_str(s)?;
        Ok(Self { scalar_type, shape })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelListWithType {
    channels: Vec<Channel>,
}

pub struct ScyllaChannelsWithType {}

impl ScyllaChannelsWithType {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/channels/with_type" {
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
                let url = Url::parse(&format!("dummy:{}", req.uri()))?;
                let q = ChannelsWithTypeQuery::from_url(&url)?;
                let res = self.get_channels(&q, node_config).await?;
                let body = Body::from(serde_json::to_vec(&res)?);
                Ok(response(StatusCode::OK).body(body)?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn get_channels(
        &self,
        q: &ChannelsWithTypeQuery,
        node_config: &NodeConfigCached,
    ) -> Result<ChannelListWithType, Error> {
        let scyco = node_config
            .node_config
            .cluster
            .scylla
            .as_ref()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("No Scylla configured")))?;
        let scy = scylla::SessionBuilder::new()
            .known_nodes(&scyco.hosts)
            .use_keyspace(&scyco.keyspace, true)
            .default_consistency(Consistency::One)
            .build()
            .await
            .err_conv()?;
        let facility = "scylla";
        let res = scy
            .query(
                "select channel_name, series from series_by_channel where facility = ? and scalar_type = ? and shape_dims = ? allow filtering",
                (facility, q.scalar_type.to_scylla_i32(), q.shape.to_scylla_vec()),
            )
            .await
            .err_conv()?;
        let mut list = Vec::new();
        for row in res.rows_typed_or_empty::<(String, i64)>() {
            let (channel_name, _series) = row.err_conv()?;
            let ch = Channel {
                backend: facility.into(),
                name: channel_name,
            };
            list.push(ch);
        }
        let ret = ChannelListWithType { channels: list };
        Ok(ret)
    }
}
