use crate::err::Error;
use crate::{response, ToPublicResponse};
use dbconn::create_connection;
use disk::binned::query::PreBinnedQuery;
use disk::events::{PlainEventsBinaryQuery, PlainEventsJsonQuery};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::query::BinnedQuery;
use netpod::{get_url_query_pairs, Channel, ChannelConfigQuery, Database, FromUrl, ScalarType, ScyllaConfig, Shape};
use netpod::{ChannelConfigResponse, NodeConfigCached};
use netpod::{ACCEPT_ALL, APP_JSON};
use scylla::batch::Consistency;
use scylla::frame::response::cql_to_rust::FromRowError as ScyFromRowError;
use scylla::transport::errors::{NewSessionError as ScyNewSessionError, QueryError as ScyQueryError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use url::Url;

pub struct ChConf {
    pub scalar_type: ScalarType,
    pub shape: Shape,
}

pub async fn chconf_from_events_binary(_q: &PlainEventsBinaryQuery, _conf: &NodeConfigCached) -> Result<ChConf, Error> {
    err::todoval()
}

pub async fn chconf_from_events_json(q: &PlainEventsJsonQuery, ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    if q.channel().backend != ncc.node_config.cluster.backend {
        warn!(
            "Mismatched backend  {}  VS  {}",
            q.channel().backend,
            ncc.node_config.cluster.backend
        );
    }
    if let Some(_conf) = &ncc.node_config.cluster.scylla {
        // This requires the series id.
        let series = q
            .channel()
            .series
            .ok_or_else(|| Error::with_msg_no_trace(format!("needs a series id")))?;
        // TODO use a common already running worker pool for these queries:
        let dbconf = &ncc.node_config.cluster.database;
        let dburl = format!(
            "postgresql://{}:{}@{}:{}/{}",
            dbconf.user, dbconf.pass, dbconf.host, dbconf.port, dbconf.name
        );
        let (pgclient, pgconn) = tokio_postgres::connect(&dburl, tokio_postgres::NoTls)
            .await
            .err_conv()?;
        tokio::spawn(pgconn);
        let res = pgclient
            .query(
                "select scalar_type, shape_dims from series_by_channel where series = $1",
                &[&(series as i64)],
            )
            .await
            .err_conv()?;
        if res.len() == 0 {
            error!("can not find channel for series {series}");
            err::todoval()
        } else if res.len() > 1 {
            error!("can not find channel for series {series}");
            err::todoval()
        } else {
            let row = res.first().unwrap();
            let scalar_type = ScalarType::from_dtype_index(row.get::<_, i32>(0) as u8)?;
            // TODO can I get a slice from psql driver?
            let shape = Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(1))?;
            let ret = ChConf { scalar_type, shape };
            Ok(ret)
        }
    } else {
        err::todoval()
    }
}

pub async fn chconf_from_prebinned(_q: &PreBinnedQuery, _conf: &NodeConfigCached) -> Result<ChConf, Error> {
    err::todoval()
}

pub async fn chconf_from_binned(_q: &BinnedQuery, _conf: &NodeConfigCached) -> Result<ChConf, Error> {
    err::todoval()
}

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

impl<T> ErrConv<T> for Result<T, tokio_postgres::Error> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

async fn config_from_scylla(
    chq: ChannelConfigQuery,
    _pgconf: Database,
    scyconf: ScyllaConfig,
    _node_config: &NodeConfigCached,
) -> Result<ChannelConfigResponse, Error> {
    // Find the "series" id.
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .use_keyspace(&scyconf.keyspace, true)
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
    let conf = if let Some(scyco) = &node_config.node_config.cluster.scylla {
        let pgconf = node_config.node_config.cluster.database.clone();
        config_from_scylla(q, pgconf, scyco.clone(), node_config).await?
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
            let (channel_name, series) = row.err_conv()?;
            let ch = Channel {
                backend: facility.into(),
                name: channel_name,
                series: Some(series as u64),
            };
            list.push(ch);
        }
        let ret = ChannelListWithType { channels: list };
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScyllaChannelsActiveQuery {
    tsedge: u64,
    shape_kind: u32,
    scalar_type: ScalarType,
}

impl FromUrl for ScyllaChannelsActiveQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        let s = pairs
            .get("tsedge")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing tsedge"))?;
        let tsedge: u64 = s.parse()?;
        let s = pairs
            .get("shape_kind")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing shape_kind"))?;
        let shape_kind: u32 = s.parse()?;
        let s = pairs
            .get("scalar_type")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing scalar_type"))?;
        let scalar_type: ScalarType = serde_json::from_str(&format!("\"{s}\""))?;
        info!("parsed scalar type  inp: {s:?}  val: {scalar_type:?}");
        Ok(Self {
            tsedge,
            scalar_type,
            shape_kind,
        })
    }
}

pub struct ScyllaChannelsActive {}

impl ScyllaChannelsActive {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/channels/active" {
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
                let q = ScyllaChannelsActiveQuery::from_url(&url)?;
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
        q: &ScyllaChannelsActiveQuery,
        node_config: &NodeConfigCached,
    ) -> Result<Vec<u64>, Error> {
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
        // Database stores tsedge/ts_msp in units of (10 sec), and we additionally map to the grid.
        let tsedge = q.tsedge / 10 / (6 * 2) * (6 * 2);
        info!(
            "ScyllaChannelsActive::get_channels  tsedge {} (10s)  {} (s)",
            tsedge,
            tsedge * 10
        );
        let mut ret = Vec::new();
        for part in 0..256 {
            let mut res = scy
            .query_iter(
                "select series from series_by_ts_msp where part = ? and ts_msp = ? and shape_kind = ? and scalar_type = ?",
                (part as i32, tsedge as i32, q.shape_kind as i32, q.scalar_type.to_scylla_i32()),
            )
            .await
            .err_conv()?;
            use futures_util::StreamExt;
            while let Some(row) = res.next().await {
                let row = row.err_conv()?;
                let (series,): (i64,) = row.into_typed().err_conv()?;
                if series == 1254561075907984640 {
                    info!(
                        "FOUND ACTIVE  series {}  part {}  tsedge {}  scalar_type {}",
                        series,
                        part,
                        tsedge,
                        q.scalar_type.to_scylla_i32()
                    );
                }
                ret.push(series as u64);
            }
        }
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelFromSeriesQuery {
    series: u64,
}

impl FromUrl for ChannelFromSeriesQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        let s = pairs
            .get("series")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing series"))?;
        let series: u64 = s.parse()?;
        Ok(Self { series })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ChannelFromSeriesResponse {
    facility: String,
    channel: String,
    scalar_type: ScalarType,
    shape: Shape,
    // TODO need a unique representation of the agg kind in the registry.
    agg_kind: u32,
}

pub struct ChannelFromSeries {}

impl ChannelFromSeries {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/channel/series" {
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
                let q = ChannelFromSeriesQuery::from_url(&url)?;
                let res = self.get_data(&q, node_config).await?;
                let body = Body::from(serde_json::to_vec(&res)?);
                Ok(response(StatusCode::OK).body(body)?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn get_data(
        &self,
        q: &ChannelFromSeriesQuery,
        node_config: &NodeConfigCached,
    ) -> Result<ChannelFromSeriesResponse, Error> {
        let series = q.series as i64;
        //let pgconn = create_connection(&node_config.node_config.cluster.database).await?;
        let dbconf = &node_config.node_config.cluster.database;
        // TODO unify the database nodes
        let uri = format!(
            "postgresql://{}:{}@{}:{}/{}",
            dbconf.user, dbconf.pass, dbconf.host, 5432, dbconf.name
        );
        let (pgclient, conn) = tokio_postgres::connect(&uri, tokio_postgres::NoTls).await.err_conv()?;
        // TODO monitor connection drop.
        let _cjh = tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {}", e);
            }
            Ok::<_, Error>(())
        });
        let res = pgclient
            .query(
                "select facility, channel, scalar_type, shape_dims, agg_kind from series_by_channel where series = $1",
                &[&series],
            )
            .await?;
        let res = if let Some(row) = res.first() {
            row
        } else {
            // TODO return code 204
            return Err(Error::with_msg_no_trace("can not find series"));
        };
        let facility: String = res.get(0);
        let channel: String = res.get(1);
        let scalar_type: i32 = res.get(2);
        // TODO check and document the format in the storage:
        let scalar_type = ScalarType::from_dtype_index(scalar_type as u8)?;
        let shape: Vec<i32> = res.get(3);
        let shape = Shape::from_scylla_shape_dims(&shape)?;
        let agg_kind: i32 = res.get(4);
        // TODO method is called from_scylla_shape_dims but document that postgres uses the same format.
        let ret = ChannelFromSeriesResponse {
            facility,
            channel,
            scalar_type,
            shape,
            agg_kind: agg_kind as u32,
        };
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IocForChannelQuery {
    channel_regex: String,
}

impl FromUrl for IocForChannelQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        let channel_regex = pairs
            .get("channel_regex")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing channel_regex"))?
            .into();
        Ok(Self { channel_regex })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelIoc {
    channel: String,
    ioc: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IocForChannelRes {
    channels: Vec<Channel>,
}

pub struct IocForChannel {}

impl IocForChannel {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/ioc/channel" {
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
                let q = IocForChannelQuery::from_url(&url)?;
                let res = self.find(&q, node_config).await?;
                let body = Body::from(serde_json::to_vec(&res)?);
                Ok(response(StatusCode::OK).body(body)?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn find(&self, q: &IocForChannelQuery, node_config: &NodeConfigCached) -> Result<IocForChannelRes, Error> {
        // TODO implement lookup in postgres
        let _ = q;
        let _pgconn = create_connection(&node_config.node_config.cluster.database).await?;
        let _facility = "scylla";
        let ret = IocForChannelRes { channels: vec![] };
        Ok(ret)
    }
}
