use crate::channelconfig::{chconf_from_events_binary, chconf_from_events_json};
use crate::err::Error;
use crate::{response, response_err, BodyStream, ToPublicResponse};
use futures_util::{Stream, StreamExt, TryStreamExt};
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use items_2::{binned_collected, ChannelEvents, ChannelEventsMerger};
use netpod::log::*;
use netpod::query::{BinnedQuery, ChannelStateEventsQuery, PlainEventsQuery};
use netpod::{AggKind, BinnedRange, FromUrl, NodeConfigCached};
use netpod::{ACCEPT_ALL, APP_JSON, APP_OCTET};
use scyllaconn::create_scy_session;
use scyllaconn::errconv::ErrConv;
use scyllaconn::events::{channel_state_events, find_series, make_scylla_stream};
use std::pin::Pin;
use std::sync::Arc;
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
    let query = PlainEventsQuery::from_url(&url)?;
    let chconf = chconf_from_events_binary(&query, node_config).await?;

    // Update the series id since we don't require some unique identifier yet.
    let mut query = query;
    query.set_series_id(chconf.series);
    let query = query;
    // ---

    let op = disk::channelexec::PlainEvents::new(query.channel().clone(), query.range().clone(), node_config.clone());
    let s = disk::channelexec::channel_exec(
        op,
        query.channel(),
        query.range(),
        chconf.scalar_type,
        chconf.shape,
        AggKind::Plain,
        node_config,
    )
    .await?;
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
    let s1 = format!("dummy:{}", head.uri);
    let url = Url::parse(&s1)?;
    let query = PlainEventsQuery::from_url(&url)?;
    let chconf = chconf_from_events_json(&query, node_config).await?;

    // Update the series id since we don't require some unique identifier yet.
    let mut query = query;
    query.set_series_id(chconf.series);
    let query = query;
    // ---

    let op = disk::channelexec::PlainEventsJson::new(
        // TODO pass only the query, not channel, range again:
        query.clone(),
        query.channel().clone(),
        query.range().clone(),
        query.timeout(),
        node_config.clone(),
        query.events_max().unwrap_or(u64::MAX),
        query.do_log(),
    );
    let s = disk::channelexec::channel_exec(
        op,
        query.channel(),
        query.range(),
        chconf.scalar_type,
        chconf.shape,
        AggKind::Plain,
        node_config,
    )
    .await?;
    let ret = response(StatusCode::OK).body(BodyStream::wrapped(
        s.map_err(Error::from),
        format!("plain_events_json"),
    ))?;
    Ok(ret)
}

pub struct EventsHandlerScylla {}

impl EventsHandlerScylla {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        match self.fetch(req, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => Ok(e.to_public_response()),
        }
    }

    async fn fetch(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        info!("EventsHandlerScylla  req: {:?}", req);
        let accept_def = APP_JSON;
        let accept = req
            .headers()
            .get(http::header::ACCEPT)
            .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
        if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
            Ok(self.gather(req, node_config).await?)
        } else {
            let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("Unsupported Accept: {:?}", accept))?;
            Ok(ret)
        }
    }

    async fn gather(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        let (head, _body) = req.into_parts();
        warn!("TODO PlainEventsQuery needs to take AggKind");
        let s1 = format!("dummy:{}", head.uri);
        let url = Url::parse(&s1)?;
        let evq = PlainEventsQuery::from_url(&url)?;
        let pgclient = {
            // TODO use common connection/pool:
            info!("---------------    open postgres connection");
            let pgconf = &node_config.node_config.cluster.database;
            let u = {
                let d = &pgconf;
                format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name)
            };
            let (pgclient, pgconn) = tokio_postgres::connect(&u, tokio_postgres::NoTls).await.err_conv()?;
            tokio::spawn(pgconn);
            let pgclient = Arc::new(pgclient);
            pgclient
        };
        let mut stream = if let Some(scyco) = &node_config.node_config.cluster.scylla {
            let scy = create_scy_session(scyco).await?;
            let (series, scalar_type, shape) = { find_series(evq.channel(), pgclient.clone()).await? };
            let stream = make_scylla_stream(&evq, series, scalar_type.clone(), shape.clone(), scy, false).await?;
            stream
        } else {
            return Err(Error::with_public_msg(format!("no scylla configured")));
        };
        let mut coll = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(k) => match k {
                    ChannelEvents::Events(mut item) => {
                        if coll.is_none() {
                            coll = Some(item.new_collector(0));
                        }
                        let cl = coll.as_mut().unwrap();
                        cl.ingest(item.as_collectable_mut());
                    }
                    ChannelEvents::Status(..) => {}
                    ChannelEvents::RangeComplete => {}
                },
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        match coll {
            Some(mut coll) => {
                let res = coll.result()?;
                let res = res.to_json_result()?;
                let res = res.to_json_bytes()?;
                let ret = response(StatusCode::OK).body(Body::from(res))?;
                Ok(ret)
            }
            None => {
                let ret = response(StatusCode::OK).body(BodyStream::wrapped(
                    futures_util::stream::iter([Ok(Vec::new())]),
                    format!("EventsHandlerScylla::gather"),
                ))?;
                Ok(ret)
            }
        }
    }
}

pub struct BinnedHandlerScylla {}

impl BinnedHandlerScylla {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/scylla/binned" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        match self.fetch(req, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => {
                eprintln!("error: {e}");
                Ok(e.to_public_response())
            }
        }
    }

    async fn fetch(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        info!("BinnedHandlerScylla  req: {:?}", req);
        let accept_def = APP_JSON;
        let accept = req
            .headers()
            .get(http::header::ACCEPT)
            .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
        if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
            Ok(self.gather(req, node_config).await?)
        } else {
            let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("Unsupported Accept: {:?}", accept))?;
            Ok(ret)
        }
    }

    async fn gather(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        let (head, _body) = req.into_parts();
        warn!("TODO BinnedQuery needs to take AggKind");
        let s1 = format!("dummy:{}", head.uri);
        let url = Url::parse(&s1)?;
        let evq = BinnedQuery::from_url(&url)?;
        let pgclient = {
            // TODO use common connection/pool:
            info!("---------------    open postgres connection");
            let pgconf = &node_config.node_config.cluster.database;
            let u = {
                let d = &pgconf;
                format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name)
            };
            let (pgclient, pgconn) = tokio_postgres::connect(&u, tokio_postgres::NoTls).await.err_conv()?;
            tokio::spawn(pgconn);
            let pgclient = Arc::new(pgclient);
            pgclient
        };
        if let Some(scyco) = &node_config.node_config.cluster.scylla {
            let scy = create_scy_session(scyco).await?;
            let mut query2 = PlainEventsQuery::new(evq.channel().clone(), evq.range().clone(), 0, None, false);
            query2.set_timeout(evq.timeout());
            let query2 = query2;
            let (series, scalar_type, shape) = { find_series(evq.channel(), pgclient.clone()).await? };
            let stream =
                make_scylla_stream(&query2, series, scalar_type.clone(), shape.clone(), scy.clone(), false).await?;
            let query3 = ChannelStateEventsQuery::new(evq.channel().clone(), evq.range().clone());
            let state_stream = channel_state_events(&query3, scy.clone())
                .await?
                .map(|x| {
                    //eprintln!("state_stream {x:?}");
                    x
                })
                .map_err(|e| items_2::Error::from(format!("{e}")));
            // TODO let the stream itself use the items_2 error, do not convert here.
            let data_stream = stream
                .map(|x| {
                    //eprintln!("data_stream {x:?}");
                    x
                })
                .map_err(|e| items_2::Error::from(format!("{e}")));
            let data_stream = Box::pin(data_stream) as _;
            let state_stream = Box::pin(state_stream) as _;
            let merged_stream = ChannelEventsMerger::new(vec![data_stream, state_stream]);
            let merged_stream = Box::pin(merged_stream) as Pin<Box<dyn Stream<Item = _> + Send>>;
            let covering = BinnedRange::covering_range(evq.range().clone(), evq.bin_count())?;
            //eprintln!("edges {:?}", covering.edges());
            // TODO return partial result if timed out.
            let binned_collected = binned_collected(
                scalar_type.clone(),
                shape.clone(),
                evq.agg_kind().clone(),
                covering.edges(),
                evq.timeout(),
                merged_stream,
            )
            .await
            .map_err(|e| Error::with_msg_no_trace(format!("{e}")))?;
            let res = binned_collected.to_json_result()?;
            let res = res.to_json_bytes()?;
            let ret = response(StatusCode::OK).body(Body::from(res))?;
            {
                let _ret = response(StatusCode::OK).body(BodyStream::wrapped(
                    futures_util::stream::iter([Ok(Vec::new())]),
                    format!("gather"),
                ))?;
            }
            Ok(ret)
        } else {
            return Err(Error::with_public_msg(format!("no scylla configured")));
        }
    }
}
