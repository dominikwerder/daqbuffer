use crate::err::Error;
use crate::response;
use crate::ToPublicResponse;
use dbconn::create_connection;
use futures_util::StreamExt;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::query::prebinned::PreBinnedQuery;
use netpod::timeunits::*;
use netpod::ChConf;
use netpod::Channel;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::Shape;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use query::api4::binned::BinnedQuery;
use query::api4::events::PlainEventsQuery;
use scylla::batch::Consistency;
use scylla::frame::response::cql_to_rust::FromRowError as ScyFromRowError;
use scylla::transport::errors::NewSessionError as ScyNewSessionError;
use scylla::transport::errors::QueryError as ScyQueryError;
use scylla::transport::iterator::NextRowError;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use url::Url;

pub async fn chconf_from_events_v1(q: &PlainEventsQuery, ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    let ret = nodenet::channelconfig::channel_config(q.range().try_into()?, q.channel().clone(), ncc).await?;
    Ok(ret)
}

pub async fn chconf_from_prebinned(q: &PreBinnedQuery, _ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    let ret = ChConf {
        backend: q.channel().backend().into(),
        series: q
            .channel()
            .series()
            .expect("PreBinnedQuery is expected to contain the series id"),
        name: q.channel().name().into(),
        scalar_type: q.scalar_type().clone(),
        shape: q.shape().clone(),
    };
    Ok(ret)
}

pub async fn chconf_from_binned(q: &BinnedQuery, ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    let ret = nodenet::channelconfig::channel_config(q.range().try_into()?, q.channel().clone(), ncc).await?;
    Ok(ret)
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
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                match self.channel_config(req, &node_config).await {
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

    async fn channel_config(
        &self,
        req: Request<Body>,
        node_config: &NodeConfigCached,
    ) -> Result<Response<Body>, Error> {
        info!("channel_config");
        let url = Url::parse(&format!("dummy:{}", req.uri()))?;
        let q = ChannelConfigQuery::from_url(&url)?;
        info!("channel_config  for q {q:?}");
        let conf = if let Some(_scyco) = &node_config.node_config.cluster.scylla {
            let c = nodenet::channelconfig::channel_config(q.range.clone(), q.channel.clone(), node_config).await?;
            ChannelConfigResponse {
                channel: Channel {
                    series: Some(c.series),
                    backend: q.channel.backend().into(),
                    name: c.name,
                },
                scalar_type: c.scalar_type,
                byte_order: None,
                shape: c.shape,
            }
        } else if let Some(_) = &node_config.node.channel_archiver {
            return Err(Error::with_msg_no_trace("no archiver"));
        } else if let Some(_) = &node_config.node.archiver_appliance {
            return Err(Error::with_msg_no_trace("no archapp"));
        } else {
            parse::channelconfig::channel_config(&q, &node_config.node).await?
        };
        let ret = response(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, APP_JSON)
            .body(Body::from(serde_json::to_string(&conf)?))?;
        Ok(ret)
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

impl<T> ErrConv<T> for Result<T, NextRowError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
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
                let res = self
                    .make_histo(&node_config.node_config.cluster.backend, node_config)
                    .await?;
                let body = Body::from(serde_json::to_vec(&res)?);
                Ok(response(StatusCode::OK).body(body)?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn make_histo(&self, backend: &str, node_config: &NodeConfigCached) -> Result<ConfigsHisto, Error> {
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
        let res = scy
            .query(
                "select scalar_type, shape_dims, series from series_by_channel where facility = ? allow filtering",
                (backend,),
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
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, err::Error> {
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
                let res = self
                    .get_channels(&q, &node_config.node_config.cluster.backend, node_config)
                    .await?;
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
        backend: &str,
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
        let res = scy
            .query(
                "select channel_name, series from series_by_channel where facility = ? and scalar_type = ? and shape_dims = ? allow filtering",
                (backend, q.scalar_type.to_scylla_i32(), q.shape.to_scylla_vec()),
            )
            .await
            .err_conv()?;
        let mut list = Vec::new();
        for row in res.rows_typed_or_empty::<(String, i64)>() {
            let (channel_name, series) = row.err_conv()?;
            let ch = Channel {
                backend: backend.into(),
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
pub struct ScyllaChannelEventSeriesIdQuery {
    backend: String,
    #[serde(rename = "channelName")]
    name: String,
    #[serde(rename = "scalarType")]
    scalar_type: ScalarType,
    shape: Shape,
    #[serde(rename = "doCreate", skip_serializing_if = "bool_false")]
    do_create: bool,
}

fn bool_false(x: &bool) -> bool {
    *x == false
}

impl FromUrl for ScyllaChannelEventSeriesIdQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, err::Error> {
        let backend = pairs
            .get("backend")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing backend"))?
            .into();
        let name = pairs
            .get("channelName")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing channelName"))?
            .into();
        let s = pairs
            .get("scalarType")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing scalarType"))?;
        let scalar_type: ScalarType = serde_json::from_str(&format!("\"{s}\""))?;
        let s = pairs
            .get("shape")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing shape"))?;
        let shape = Shape::from_dims_str(s)?;
        let do_create = pairs.get("doCreate").map_or("false", |x| x.as_str()) == "true";
        Ok(Self {
            backend,
            name,
            scalar_type,
            shape,
            do_create,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScyllaChannelEventSeriesIdResponse {
    #[serde(rename = "seriesId")]
    series: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScyllaChannelsActiveQuery {
    tsedge: u64,
    #[serde(rename = "shapeKind")]
    shape_kind: u32,
    #[serde(rename = "scalarType")]
    scalar_type: ScalarType,
}

impl FromUrl for ScyllaChannelsActiveQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, err::Error> {
        let s = pairs
            .get("tsedge")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing tsedge"))?;
        let tsedge: u64 = s.parse()?;
        let s = pairs
            .get("shapeKind")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing shapeKind"))?;
        let shape_kind: u32 = s.parse()?;
        let s = pairs
            .get("scalarType")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing scalarType"))?;
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
        if req.uri().path() == "/api/4/channels/active" {
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
            while let Some(row) = res.next().await {
                let row = row.err_conv()?;
                let (series,): (i64,) = row.into_typed().err_conv()?;
                ret.push(series as u64);
            }
        }
        Ok(ret)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct IocForChannelQuery {
    #[serde(rename = "backend")]
    backend: String,
    #[serde(rename = "channelName")]
    name: String,
}

impl FromUrl for IocForChannelQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, err::Error> {
        let backend = pairs
            .get("backend")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing backend"))?
            .into();
        let name = pairs
            .get("channelName")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing channelName"))?
            .into();
        Ok(Self { backend, name })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IocForChannelRes {
    #[serde(rename = "iocAddr")]
    ioc_addr: String,
}

pub struct IocForChannel {}

impl IocForChannel {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/channel/ioc" {
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
                match self.find(&q, node_config).await {
                    Ok(k) => {
                        let body = Body::from(serde_json::to_vec(&k)?);
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => {
                        let body = Body::from(format!("{:?}", e.public_msg()));
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body)?)
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }

    async fn find(
        &self,
        q: &IocForChannelQuery,
        node_config: &NodeConfigCached,
    ) -> Result<Option<IocForChannelRes>, Error> {
        let dbconf = &node_config.node_config.cluster.database;
        let pg_client = create_connection(dbconf).await?;
        let rows = pg_client
            .query(
                "select addr from ioc_by_channel where facility = $1 and channel = $2",
                &[&q.backend, &q.name],
            )
            .await?;
        if let Some(row) = rows.first() {
            let ioc_addr = row.get(0);
            let ret = IocForChannelRes { ioc_addr };
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ScyllaSeriesTsMspQuery {
    #[serde(rename = "seriesId")]
    series: u64,
}

impl FromUrl for ScyllaSeriesTsMspQuery {
    fn from_url(url: &Url) -> Result<Self, err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, err::Error> {
        let s = pairs
            .get("seriesId")
            .ok_or_else(|| Error::with_public_msg_no_trace("missing seriesId"))?;
        let series: u64 = s.parse()?;
        Ok(Self { series })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScyllaSeriesTsMspResponse {
    #[serde(rename = "tsMsps")]
    ts_msps: Vec<u64>,
}

pub struct ScyllaSeriesTsMsp {}

impl ScyllaSeriesTsMsp {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/series/tsMsps" {
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
                let q = ScyllaSeriesTsMspQuery::from_url(&url)?;
                match self.get_ts_msps(&q, node_config).await {
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

    async fn get_ts_msps(
        &self,
        q: &ScyllaSeriesTsMspQuery,
        node_config: &NodeConfigCached,
    ) -> Result<ScyllaSeriesTsMspResponse, Error> {
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
        let mut ts_msps = Vec::new();
        let mut res = scy
            .query_iter("select ts_msp from ts_msp where series = ?", (q.series as i64,))
            .await
            .err_conv()?;
        while let Some(row) = res.next().await {
            let row = row.err_conv()?;
            let (ts_msp,): (i64,) = row.into_typed().err_conv()?;
            ts_msps.push(ts_msp as u64);
        }
        let ret = ScyllaSeriesTsMspResponse { ts_msps };
        Ok(ret)
    }
}

#[derive(Serialize)]
pub struct AmbigiousChannel {
    series: u64,
    name: String,
    scalar_type: ScalarType,
    shape: Shape,
}

#[derive(Serialize)]
pub struct AmbigiousChannelNamesResponse {
    ambigious: Vec<AmbigiousChannel>,
}

pub struct AmbigiousChannelNames {}

impl AmbigiousChannelNames {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/channels/ambigious" {
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
                match self.process(node_config).await {
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

    async fn process(&self, node_config: &NodeConfigCached) -> Result<AmbigiousChannelNamesResponse, Error> {
        let dbconf = &node_config.node_config.cluster.database;
        let pg_client = create_connection(dbconf).await?;
        let rows = pg_client
            .query(
                "select t2.series, t2.channel, t2.scalar_type, t2.shape_dims, t2.agg_kind from series_by_channel t1, series_by_channel t2 where t2.channel = t1.channel and t2.series != t1.series",
                &[],
            )
            .await?;
        let mut ret = AmbigiousChannelNamesResponse { ambigious: Vec::new() };
        for row in rows {
            let g = AmbigiousChannel {
                series: row.get::<_, i64>(0) as u64,
                name: row.get(1),
                scalar_type: ScalarType::from_scylla_i32(row.get(2))?,
                shape: Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(3))?,
            };
            ret.ambigious.push(g);
        }
        Ok(ret)
    }
}

struct TestData01Iter {}

impl Iterator for TestData01Iter {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

struct Msps(Vec<u64>);
struct Lsps(Vec<u64>);
struct Pulses(Vec<u64>);
struct ValsF64(Vec<f64>);

fn test_data_f64_01() -> (Msps, Lsps, Pulses, ValsF64) {
    let mut msps = Msps(Vec::new());
    let mut lsps = Lsps(Vec::new());
    let mut pulses = Pulses(Vec::new());
    let mut vals = ValsF64(Vec::new());
    let mut msp = 0;
    let mut i1 = 0;
    for i in 0..2000 {
        let ts = SEC * 1600000000 + MIN * 2 * i;
        let pulse = 10000 + i;
        if msp == 0 || i1 >= 40 {
            msp = ts / MIN * MIN;
            i1 = 0;
        }
        msps.0.push(msp);
        lsps.0.push(ts - msp);
        pulses.0.push(pulse);
        vals.0.push(pulse as f64 + 0.4 + 0.2 * (pulse as f64).sin());
        i1 += 1;
    }
    (msps, lsps, pulses, vals)
}

pub struct GenerateScyllaTestData {}

impl GenerateScyllaTestData {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/test/generate/scylla" {
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
                match self.process(node_config).await {
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

    async fn process(&self, node_config: &NodeConfigCached) -> Result<(), Error> {
        let dbconf = &node_config.node_config.cluster.database;
        let _pg_client = create_connection(dbconf).await?;
        let scyconf = node_config.node_config.cluster.scylla.as_ref().unwrap();
        let scy = scyllaconn::create_scy_session(scyconf).await?;
        let series: u64 = 42001;
        // TODO query `ts_msp` for all MSP values und use that to delete from event table first.
        // Only later delete also from the `ts_msp` table.
        let it = scy
            .query_iter("select ts_msp from ts_msp where series = ?", (series as i64,))
            .await
            .err_conv()?;
        let mut it = it.into_typed::<(i64,)>();
        while let Some(row) = it.next().await {
            let row = row.err_conv()?;
            let values = (series as i64, row.0);
            scy.query("delete from events_scalar_f64 where series = ? and ts_msp = ?", values)
                .await
                .err_conv()?;
        }
        scy.query("delete from ts_msp where series = ?", (series as i64,))
            .await
            .err_conv()?;

        // Generate
        let (msps, lsps, pulses, vals) = test_data_f64_01();
        let mut last = 0;
        for msp in msps.0.iter().map(|x| *x) {
            if msp != last {
                scy.query(
                    "insert into ts_msp (series, ts_msp) values (?, ?)",
                    (series as i64, msp as i64),
                )
                .await
                .err_conv()?;
            }
            last = msp;
        }
        for (((msp, lsp), pulse), val) in msps.0.into_iter().zip(lsps.0).zip(pulses.0).zip(vals.0) {
            scy.query(
                "insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)",
                (series as i64, msp as i64, lsp as i64, pulse as i64, val),
            )
            .await
            .err_conv()?;
        }
        Ok(())
    }
}
