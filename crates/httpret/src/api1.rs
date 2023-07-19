use crate::err::Error;
use crate::gather::gather_get_json_generic;
use crate::gather::SubRes;
use crate::response;
use crate::ReqCtx;
use bytes::BufMut;
use bytes::BytesMut;
use disk::eventchunker::EventChunkerConf;
use disk::merge::mergedblobsfromremotes::MergedBlobsFromRemotes;
use disk::raw::conn::make_local_event_blobs_stream;
use futures_util::Stream;
use futures_util::StreamExt;
use http::Method;
use http::StatusCode;
use hyper::Body;
use hyper::Client;
use hyper::Request;
use hyper::Response;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::WithLen;
use items_2::eventfull::EventFull;
use itertools::Itertools;
use netpod::log::*;
use netpod::query::api1::Api1Query;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::SEC;
use netpod::Api1WarningStats;
use netpod::ByteSize;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::ChannelTypeConfigGen;
use netpod::DiskIoTune;
use netpod::NodeConfigCached;
use netpod::ProxyConfig;
use netpod::ReqCtxArc;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use netpod::APP_OCTET;
use netpod::X_DAQBUF_REQID;
use parse::api1_parse::Api1ByteOrder;
use parse::api1_parse::Api1ChannelHeader;
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::any;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use tracing_futures::Instrument;
use url::Url;

pub trait BackendAware {
    fn backend(&self) -> &str;
}

pub trait FromErrorCode {
    fn from_error_code(backend: &str, code: ErrorCode) -> Self;
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ErrorCode {
    Error,
    Timeout,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrorDescription {
    code: ErrorCode,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Ordering {
    #[serde(rename = "none")]
    NONE,
    #[serde(rename = "asc")]
    ASC,
    #[serde(rename = "desc")]
    DESC,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelSearchQueryV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    #[serde(rename = "sourceRegex", skip_serializing_if = "Option::is_none")]
    pub source_regex: Option<String>,
    #[serde(rename = "descriptionRegex", skip_serializing_if = "Option::is_none")]
    pub description_regex: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub backends: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering: Option<Ordering>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchResultItemV1 {
    pub backend: String,
    pub channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDescription>,
}

impl BackendAware for ChannelSearchResultItemV1 {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl FromErrorCode for ChannelSearchResultItemV1 {
    fn from_error_code(backend: &str, code: ErrorCode) -> Self {
        Self {
            backend: backend.into(),
            channels: vec![],
            error: Some(ErrorDescription { code }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchResultV1(pub Vec<ChannelSearchResultItemV1>);

pub async fn channel_search_list_v1(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, reqbody) = req.into_parts();
    let bodybytes = hyper::body::to_bytes(reqbody).await?;
    let query: ChannelSearchQueryV1 = serde_json::from_slice(&bodybytes)?;
    match head.headers.get(http::header::ACCEPT) {
        Some(v) => {
            if v == APP_JSON {
                let query = ChannelSearchQuery {
                    // TODO
                    backend: None,
                    name_regex: query.regex.map_or(String::new(), |k| k),
                    source_regex: query.source_regex.map_or(String::new(), |k| k),
                    description_regex: query.description_regex.map_or(String::new(), |k| k),
                };
                let urls = proxy_config
                    .backends
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh.url)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(e) => Err(Error::with_msg(format!("parse error for: {:?}  {:?}", sh, e))),
                    })
                    .fold_ok(vec![], |mut a, x| {
                        a.push(x);
                        a
                    })?;
                let tags: Vec<_> = urls.iter().map(|k| k.to_string()).collect();
                let nt = |tag, res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        let ret = SubRes {
                            tag,
                            status: StatusCode::OK,
                            val: res,
                        };
                        Ok(ret)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<(crate::gather::Tag, Result<SubRes<ChannelSearchResult>, Error>)>| {
                    let mut res = ChannelSearchResultV1(Vec::new());
                    for (_tag, j) in all {
                        match j {
                            Ok(j) => {
                                for k in j.val.channels {
                                    let mut found = false;
                                    let mut i2 = 0;
                                    for i1 in 0..res.0.len() {
                                        if res.0[i1].backend == k.backend {
                                            found = true;
                                            i2 = i1;
                                            break;
                                        }
                                    }
                                    if !found {
                                        let u = ChannelSearchResultItemV1 {
                                            backend: k.backend,
                                            channels: Vec::new(),
                                            error: None,
                                        };
                                        res.0.push(u);
                                        i2 = res.0.len() - 1;
                                    }
                                    res.0[i2].channels.push(k.name);
                                }
                            }
                            Err(e) => {
                                warn!("{e}");
                            }
                        }
                    }
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, APP_JSON)
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                let bodies = (0..urls.len()).into_iter().map(|_| None).collect();
                let ret = gather_get_json_generic(
                    http::Method::GET,
                    urls,
                    bodies,
                    tags,
                    nt,
                    ft,
                    Duration::from_millis(3000),
                )
                .await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

pub async fn channel_search_configs_v1(
    req: Request<Body>,
    proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error> {
    let (head, reqbody) = req.into_parts();
    let bodybytes = hyper::body::to_bytes(reqbody).await?;
    let query: ChannelSearchQueryV1 = serde_json::from_slice(&bodybytes)?;
    match head.headers.get(http::header::ACCEPT) {
        Some(v) => {
            if v == APP_JSON {
                // Transform the ChannelSearchQueryV1 to ChannelSearchQuery
                let query = ChannelSearchQuery {
                    // TODO
                    backend: None,
                    name_regex: query.regex.map_or(String::new(), |k| k),
                    source_regex: query.source_regex.map_or(String::new(), |k| k),
                    description_regex: query.description_regex.map_or(String::new(), |k| k),
                };
                let urls = proxy_config
                    .backends
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh.url)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(e) => Err(Error::with_msg(format!("parse error for: {:?}  {:?}", sh, e))),
                    })
                    .fold_ok(vec![], |mut a, x| {
                        a.push(x);
                        a
                    })?;
                let tags: Vec<_> = urls.iter().map(|k| k.to_string()).collect();
                let nt = |tag, res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        let ret = SubRes {
                            tag,
                            status: StatusCode::OK,
                            val: res,
                        };
                        Ok(ret)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<(crate::gather::Tag, Result<SubRes<ChannelSearchResult>, Error>)>| {
                    let mut res = ChannelConfigsResponseV1(Vec::new());
                    for (_tag, j) in all {
                        match j {
                            Ok(j) => {
                                for k in j.val.channels {
                                    let mut found = false;
                                    let mut i2 = 0;
                                    for i1 in 0..res.0.len() {
                                        if res.0[i1].backend == k.backend {
                                            found = true;
                                            i2 = i1;
                                            break;
                                        }
                                    }
                                    if !found {
                                        let u = ChannelBackendConfigsV1 {
                                            backend: k.backend.clone(),
                                            channels: Vec::new(),
                                            error: None,
                                        };
                                        res.0.push(u);
                                        i2 = res.0.len() - 1;
                                    }
                                    {
                                        let shape = if k.shape.len() == 0 { None } else { Some(k.shape) };
                                        let unit = if k.unit.len() == 0 { None } else { Some(k.unit) };
                                        let description = if k.description.len() == 0 {
                                            None
                                        } else {
                                            Some(k.description)
                                        };
                                        let t = ChannelConfigV1 {
                                            backend: k.backend,
                                            name: k.name,
                                            source: k.source,
                                            description,
                                            ty: k.ty,
                                            shape,
                                            unit,
                                        };
                                        res.0[i2].channels.push(t);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("{e}");
                            }
                        }
                    }
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, APP_JSON)
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                let bodies = (0..urls.len()).into_iter().map(|_| None).collect();
                let ret = gather_get_json_generic(
                    http::Method::GET,
                    urls,
                    bodies,
                    tags,
                    nt,
                    ft,
                    Duration::from_millis(3000),
                )
                .await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigV1 {
    pub backend: String,
    pub name: String,
    pub source: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shape: Option<Vec<u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigsQueryV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    #[serde(rename = "sourceRegex")]
    pub source_regex: Option<String>,
    #[serde(rename = "descriptionRegex")]
    pub description_regex: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub backends: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering: Option<Ordering>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelBackendConfigsV1 {
    pub backend: String,
    pub channels: Vec<ChannelConfigV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDescription>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfigsResponseV1(pub Vec<ChannelBackendConfigsV1>);

impl BackendAware for ChannelBackendConfigsV1 {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl FromErrorCode for ChannelBackendConfigsV1 {
    fn from_error_code(backend: &str, code: ErrorCode) -> Self {
        Self {
            backend: backend.into(),
            channels: vec![],
            error: Some(ErrorDescription { code }),
        }
    }
}

// TODO replace usage of this by gather-generic
pub async fn gather_json_2_v1(
    req: Request<Body>,
    pathpre: &str,
    _proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error> {
    let (part_head, part_body) = req.into_parts();
    let bodyslice = hyper::body::to_bytes(part_body).await?;
    let gather_from: GatherFromV1 = serde_json::from_slice(&bodyslice)?;
    let mut spawned = vec![];
    let uri = part_head.uri;
    let path_post = &uri.path()[pathpre.len()..];
    //let hds = part_head.headers;
    for gh in gather_from.hosts {
        let uri = format!("http://{}:{}/{}", gh.host, gh.port, path_post);
        let req = Request::builder().method(Method::GET).uri(uri);
        let req = if gh.inst.len() > 0 {
            req.header("retrieval_instance", &gh.inst)
        } else {
            req
        };
        let req = req.header(http::header::ACCEPT, APP_JSON);
        //.body(Body::from(serde_json::to_string(&q)?))?;
        let req = req.body(Body::empty());
        let task = tokio::spawn(async move {
            //let res = Client::new().request(req);
            let res = Client::new().request(req?).await;
            Ok::<_, Error>(process_answer(res?).await?)
        });
        let task = tokio::time::timeout(std::time::Duration::from_millis(5000), task);
        spawned.push((gh.clone(), task));
    }
    #[derive(Serialize)]
    struct Hres {
        gh: GatherHostV1,
        res: JsonValue,
    }
    #[derive(Serialize)]
    struct Jres {
        hosts: Vec<Hres>,
    }
    let mut a = Vec::new();
    for tr in spawned {
        let res = match tr.1.await {
            Ok(k) => match k {
                Ok(k) => match k {
                    Ok(k) => k,
                    Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
                },
                Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
            },
            Err(e) => JsonValue::String(format!("ERROR({:?})", e)),
        };
        a.push(Hres { gh: tr.0, res });
    }
    let res = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON)
        .body(serde_json::to_string(&Jres { hosts: a })?.into())?;
    Ok(res)
}

#[derive(Clone, Serialize, Deserialize)]
struct GatherFromV1 {
    hosts: Vec<GatherHostV1>,
}

#[derive(Clone, Serialize, Deserialize)]
struct GatherHostV1 {
    host: String,
    port: u16,
    inst: String,
}

async fn process_answer(res: Response<Body>) -> Result<JsonValue, Error> {
    let (pre, mut body) = res.into_parts();
    if pre.status != StatusCode::OK {
        use hyper::body::HttpBody;
        if let Some(c) = body.data().await {
            let c: bytes::Bytes = c?;
            let s1 = String::from_utf8(c.to_vec())?;
            Ok(JsonValue::String(format!(
                "status {}  body {}",
                pre.status.as_str(),
                s1
            )))
        } else {
            Ok(JsonValue::String(format!("status {}", pre.status.as_str())))
        }
    } else {
        let body: hyper::Body = body;
        let body_all = hyper::body::to_bytes(body).await?;
        let val = match serde_json::from_slice(&body_all) {
            Ok(k) => k,
            Err(_e) => JsonValue::String(String::from_utf8(body_all.to_vec())?),
        };
        Ok::<_, Error>(val)
    }
}

pub struct DataApiPython3DataStream {
    range: NanoRange,
    channels: VecDeque<ChannelTypeConfigGen>,
    settings: EventsSubQuerySettings,
    current_channel: Option<ChannelTypeConfigGen>,
    current_fetch_info: Option<SfChFetchInfo>,
    node_config: NodeConfigCached,
    chan_stream: Option<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>>,
    disk_io_tune: DiskIoTune,
    do_decompress: bool,
    event_count: usize,
    events_max: u64,
    header_out: bool,
    reqctx: ReqCtxArc,
    ping_last: Instant,
    data_done: bool,
    completed: bool,
    stats: Api1WarningStats,
}

impl DataApiPython3DataStream {
    pub fn new(
        range: NanoRange,
        channels: Vec<ChannelTypeConfigGen>,
        settings: EventsSubQuerySettings,
        disk_io_tune: DiskIoTune,
        do_decompress: bool,
        events_max: u64,
        reqctx: ReqCtxArc,
        node_config: NodeConfigCached,
    ) -> Self {
        Self {
            range,
            channels: channels.into_iter().collect(),
            settings,
            current_channel: None,
            current_fetch_info: None,
            node_config,
            chan_stream: None,
            disk_io_tune,
            do_decompress,
            event_count: 0,
            events_max,
            header_out: false,
            reqctx,
            ping_last: Instant::now(),
            data_done: false,
            completed: false,
            stats: Api1WarningStats::new(),
        }
    }

    fn channel_finished(&mut self) {
        self.chan_stream = None;
        self.header_out = false;
        self.event_count = 0;
    }

    fn convert_item(
        b: EventFull,
        channel: &ChannelTypeConfigGen,
        fetch_info: &SfChFetchInfo,
        do_decompress: bool,
        header_out: &mut bool,
        count_events: &mut usize,
    ) -> Result<BytesMut, Error> {
        let shape = fetch_info.shape();
        let mut d = BytesMut::new();
        for i1 in 0..b.len() {
            const EVIMAX: usize = 20;
            if *count_events < EVIMAX {
                debug!(
                    "ev info {}/{} bloblen {:?}  BE {:?}  scalar-type {:?}  shape {:?}  comps {:?}",
                    *count_events + 1,
                    EVIMAX,
                    b.blobs[i1].len(),
                    b.be[i1],
                    b.scalar_types[i1],
                    b.shapes[i1],
                    b.comps[i1],
                );
            }
            // TODO emit warning when we use a different setting compared to channel config.
            if false {
                let _compression = if let (Shape::Image(..), Some(..)) = (&b.shapes[i1], &b.comps[i1]) {
                    Some(1)
                } else {
                    None
                };
            };
            if !*header_out {
                let byte_order = if b.be[i1] {
                    Api1ByteOrder::Big
                } else {
                    Api1ByteOrder::Little
                };
                let head = Api1ChannelHeader::new(
                    channel.name().into(),
                    b.scalar_types.get(i1).unwrap().into(),
                    byte_order,
                    shape.clone(),
                    b.comps.get(i1).map(|x| x.clone()).unwrap(),
                );
                let h = serde_json::to_string(&head)?;
                debug!("sending channel header {}", h);
                let l1 = 1 + h.as_bytes().len() as u32;
                d.put_u32(l1);
                d.put_u8(0);
                debug!("header frame byte len {}", 4 + 1 + h.as_bytes().len());
                d.extend_from_slice(h.as_bytes());
                d.put_u32(l1);
                *header_out = true;
            }
            match &b.shapes[i1] {
                _ => {
                    if do_decompress {
                        let blob = b
                            .data_decompressed(i1, fetch_info.scalar_type(), fetch_info.shape())
                            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
                        let l1 = 17 + blob.len() as u32;
                        d.put_u32(l1);
                        d.put_u8(1);
                        d.put_u64(b.tss[i1]);
                        d.put_u64(b.pulses[i1]);
                        d.put_slice(&blob);
                        d.put_u32(l1);
                    } else {
                        let blob = b.data_raw(i1);
                        let l1 = 17 + blob.len() as u32;
                        d.put_u32(l1);
                        d.put_u8(1);
                        d.put_u64(b.tss[i1]);
                        d.put_u64(b.pulses[i1]);
                        d.put_slice(&blob);
                        d.put_u32(l1);
                    }
                }
            }
            *count_events += 1;
        }
        Ok(d)
    }

    fn handle_chan_stream_ready(&mut self, item: Sitemty<EventFull>) -> Result<BytesMut, Error> {
        let ret = match item {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::RangeComplete => {
                        debug!("sees RangeComplete");
                        Ok(BytesMut::new())
                    }
                    RangeCompletableItem::Data(k) => {
                        self.event_count += k.len();
                        if self.events_max != 0 && self.event_count >= self.events_max as usize {
                            return Err(Error::with_msg_no_trace(format!(
                                "events_max reached  {}  {}",
                                self.event_count, self.events_max
                            )));
                        }

                        // NOTE needed because the databuffer actually doesn't write
                        // the correct shape per event.
                        let mut k = k;
                        if let Some(fi) = self.current_fetch_info.as_ref() {
                            if let Shape::Scalar = fi.shape() {
                            } else {
                                k.overwrite_all_shapes(fi.shape());
                            }
                        }
                        let k = k;

                        let item = Self::convert_item(
                            k,
                            self.current_channel.as_ref().unwrap(),
                            self.current_fetch_info.as_ref().unwrap(),
                            self.do_decompress,
                            &mut self.header_out,
                            &mut self.event_count,
                        )?;
                        Ok(item)
                    }
                },
                StreamItem::Log(k) => {
                    let nodeix = k.node_ix;
                    if k.level == Level::ERROR {
                        tracing::event!(Level::ERROR, nodeix, message = k.msg);
                    } else if k.level == Level::WARN {
                        tracing::event!(Level::WARN, nodeix, message = k.msg);
                    } else if k.level == Level::INFO {
                        tracing::event!(Level::INFO, nodeix, message = k.msg);
                    } else if k.level == Level::DEBUG {
                        tracing::event!(Level::DEBUG, nodeix, message = k.msg);
                    } else if k.level == Level::TRACE {
                        tracing::event!(Level::TRACE, nodeix, message = k.msg);
                    } else {
                        tracing::event!(Level::TRACE, nodeix, message = k.msg);
                    }
                    Ok(BytesMut::new())
                }
                StreamItem::Stats(k) => {
                    //
                    Ok(BytesMut::new())
                }
            },
            Err(e) => {
                error!("DataApiPython3DataStream  emit error: {e}");
                Err(e.into())
            }
        };
        let tsnow = Instant::now();
        if tsnow.duration_since(self.ping_last) >= Duration::from_millis(500) {
            self.ping_last = tsnow;
            let mut sb = crate::status_board().unwrap();
            sb.mark_alive(self.reqctx.reqid());
        }
        ret
    }

    // TODO this stream can currently only handle sf-databuffer type backend anyway.
    fn handle_config_fut_ready(&mut self, fetch_info: SfChFetchInfo) -> Result<(), Error> {
        let select = EventsSubQuerySelect::new(
            ChannelTypeConfigGen::SfDatabuffer(fetch_info.clone()),
            self.range.clone().into(),
            TransformQuery::for_event_blobs(),
        );
        let subq = EventsSubQuery::from_parts(select, self.settings.clone(), self.reqctx.reqid().into());
        let one_before = subq.transform().need_one_before_range();
        debug!("query for event blobs retrieval  subq {subq:?}");
        // TODO  important TODO
        debug!("TODO fix magic inmem_bufcap");
        debug!("TODO add timeout option to data api3 download");
        // TODO is this a good to place decide this?
        let stream = if self.node_config.node_config.cluster.is_central_storage {
            debug!("set up central storage stream");
            // TODO pull up this config
            let event_chunker_conf = EventChunkerConf::new(ByteSize::from_kb(1024));
            let s = make_local_event_blobs_stream(
                self.range.clone(),
                fetch_info.clone(),
                one_before,
                event_chunker_conf,
                self.disk_io_tune.clone(),
                self.reqctx.clone(),
                &self.node_config,
            )?;
            Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>
        } else {
            debug!("set up merged remote stream  {}", fetch_info.name());
            let s = MergedBlobsFromRemotes::new(subq, self.node_config.node_config.cluster.clone());
            Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>
        };
        self.chan_stream = Some(Box::pin(stream));
        self.current_fetch_info = Some(fetch_info);
        Ok(())
    }
}

impl Stream for DataApiPython3DataStream {
    type Item = Result<BytesMut, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("poll on completed")
            } else if self.data_done {
                self.completed = true;
                Ready(None)
            } else {
                if let Some(stream) = &mut self.chan_stream {
                    match stream.poll_next_unpin(cx) {
                        Ready(Some(k)) => match self.handle_chan_stream_ready(k) {
                            Ok(k) => Ready(Some(Ok(k))),
                            Err(e) => {
                                error!("{e}");
                                self.chan_stream = None;
                                self.current_channel = None;
                                self.current_fetch_info = None;
                                self.data_done = true;
                                let mut sb = crate::status_board().unwrap();
                                sb.add_error(self.reqctx.reqid(), e.0.clone());
                                Ready(Some(Err(e)))
                            }
                        },
                        Ready(None) => {
                            self.channel_finished();
                            continue;
                        }
                        Pending => Pending,
                    }
                } else {
                    if let Some(chconf) = self.channels.pop_front() {
                        match &chconf {
                            ChannelTypeConfigGen::Scylla(_) => {
                                // TODO count
                                continue;
                            }
                            ChannelTypeConfigGen::SfDatabuffer(k) => match self.handle_config_fut_ready(k.clone()) {
                                Ok(()) => {
                                    self.current_channel = Some(chconf.clone());
                                    continue;
                                }
                                Err(e) => {
                                    error!("api1_binary_events  error {:?}", e);
                                    self.stats.subreq_fail += 1;
                                    continue;
                                }
                            },
                        }
                    } else {
                        self.data_done = true;
                        {
                            let n = Instant::now();
                            self.ping_last = n;
                            let mut sb = crate::status_board().unwrap();
                            sb.mark_alive(self.reqctx.reqid());
                            sb.mark_done(self.reqctx.reqid());
                        }
                        continue;
                    }
                }
            };
        }
    }
}

#[allow(unused)]
fn shape_to_api3proto(sh: &Option<Vec<u32>>) -> Vec<u32> {
    match sh {
        None => vec![],
        Some(g) => {
            if g.len() == 1 {
                vec![g[0]]
            } else if g.len() == 2 {
                vec![g[0], g[1]]
            } else {
                err::todoval()
            }
        }
    }
}

pub struct Api1EventsBinaryHandler {}

impl Api1EventsBinaryHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/1/query" {
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
        if req.method() != Method::POST {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?);
        }
        let (head, body) = req.into_parts();
        let accept = head
            .headers
            .get(http::header::ACCEPT)
            .map_or(Ok(ACCEPT_ALL), |k| k.to_str())
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?
            .to_owned();
        let body_data = hyper::body::to_bytes(body).await?;
        if body_data.len() < 1024 * 2 && body_data.first() == Some(&"{".as_bytes()[0]) {
            debug!("request body_data string: {}", String::from_utf8_lossy(&body_data));
        }
        let qu = match serde_json::from_slice::<Api1Query>(&body_data) {
            Ok(x) => x,
            Err(e) => {
                error!("got body_data: {:?}", String::from_utf8_lossy(&body_data[..]));
                error!("can not parse: {e}");
                return Err(Error::with_msg_no_trace("can not parse query"));
            }
        };
        let reqid = super::status_board()?.new_status_id();
        let reqctx = netpod::ReqCtx::new(reqid);
        let span = if qu.log_level() == "trace" {
            debug!("enable trace for handler");
            tracing::span!(tracing::Level::TRACE, "log_span_trace")
        } else if qu.log_level() == "debug" {
            debug!("enable debug for handler");
            tracing::span!(tracing::Level::DEBUG, "log_span_debug")
        } else {
            tracing::Span::none()
        };
        let reqidspan = tracing::info_span!("api1query", reqid = reqctx.reqid());
        self.handle_for_query(qu, accept, &reqctx, span.clone(), reqidspan.clone(), node_config)
            .instrument(span)
            .instrument(reqidspan)
            .await
    }

    pub async fn handle_for_query(
        &self,
        qu: Api1Query,
        accept: String,
        reqctx: &ReqCtxArc,
        span: tracing::Span,
        reqidspan: tracing::Span,
        ncc: &NodeConfigCached,
    ) -> Result<Response<Body>, Error> {
        let self_name = any::type_name::<Self>();
        // TODO this should go to usage statistics:
        debug!(
            "{self_name}  {:?}  {}  {:?}",
            qu.range(),
            qu.channels().len(),
            qu.channels().first()
        );
        let settings = EventsSubQuerySettings::from(&qu);
        let beg_date = qu.range().beg().clone();
        let end_date = qu.range().end().clone();
        trace!("{self_name}  beg_date {:?}  end_date {:?}", beg_date, end_date);
        //let url = Url::parse(&format!("dummy:{}", req.uri()))?;
        //let query = PlainEventsBinaryQuery::from_url(&url)?;
        if accept.contains(APP_OCTET) || accept.contains(ACCEPT_ALL) {
            let beg = beg_date.timestamp() as u64 * SEC + beg_date.timestamp_subsec_nanos() as u64;
            let end = end_date.timestamp() as u64 * SEC + end_date.timestamp_subsec_nanos() as u64;
            let range = NanoRange { beg, end };
            // TODO check for valid given backend name:
            let backend = &ncc.node_config.cluster.backend;
            // TODO ask for channel config quorum for all channels up front.
            //httpclient::http_get(url, accept);
            let ts1 = Instant::now();
            let mut chans = Vec::new();
            for ch in qu.channels() {
                debug!("try to find config quorum for {ch:?}");
                let ch = SfDbChannel::from_name(backend, ch.name());
                let ch_conf =
                    nodenet::configquorum::find_config_basics_quorum(ch.clone(), range.clone().into(), ncc).await?;
                match ch_conf {
                    Some(x) => {
                        debug!("found quorum {ch:?} {x:?}");
                        chans.push(x);
                    }
                    None => {
                        // TODO count in request ctx.
                        // TODO must already here have the final stats counter container.
                        // This means, the request status must provide these counters.
                        error!("no config quorum found for {ch:?}");
                        let mut sb = crate::status_board().unwrap();
                        sb.mark_alive(reqctx.reqid());
                        if let Some(e) = sb.get_entry(reqctx.reqid()) {
                            e.channel_not_found_inc();
                        }
                    }
                }
            }
            let ts2 = Instant::now();
            let dt = ts2.duration_since(ts1).as_millis();
            debug!("{self_name}  {} configs fetched in {} ms", chans.len(), dt);
            let s = DataApiPython3DataStream::new(
                range.clone(),
                chans,
                // TODO carry those settings from the query again
                settings,
                DiskIoTune::default(),
                qu.decompress()
                    .unwrap_or_else(|| ncc.node_config.cluster.decompress_default()),
                qu.events_max().unwrap_or(u64::MAX),
                reqctx.clone(),
                ncc.clone(),
            );
            let s = s.instrument(span).instrument(reqidspan);
            let body = Body::wrap_stream(s);
            let ret = response(StatusCode::OK).header(X_DAQBUF_REQID, reqctx.reqid());
            let ret = ret.body(body)?;
            Ok(ret)
        } else {
            // TODO set the public error code and message and return Err(e).
            let e = Error::with_public_msg(format!("{self_name}  unsupported Accept: {}", accept));
            error!("{self_name}  {e}");
            Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
        }
    }
}

pub struct RequestStatusHandler {}

impl RequestStatusHandler {
    pub fn path_prefix() -> &'static str {
        "/api/1/requestStatus/"
    }

    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, _ncc: &NodeConfigCached) -> Result<Response<Body>, Error> {
        let (head, body) = req.into_parts();
        if head.method != Method::GET {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?);
        }
        let accept = head
            .headers
            .get(http::header::ACCEPT)
            .map_or(Ok(ACCEPT_ALL), |k| k.to_str())
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?
            .to_owned();
        if accept != APP_JSON && accept != ACCEPT_ALL {
            // TODO set the public error code and message and return Err(e).
            let e = Error::with_public_msg(format!("Unsupported Accept: {:?}", accept));
            error!("{e}");
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let _body_data = hyper::body::to_bytes(body).await?;
        let status_id = &head.uri.path()[Self::path_prefix().len()..];
        debug!("RequestStatusHandler  status_id {:?}", status_id);
        let status = crate::status_board()?.status_as_json(status_id);
        let s = serde_json::to_string(&status)?;
        let ret = response(StatusCode::OK).body(Body::from(s))?;
        Ok(ret)
    }
}
