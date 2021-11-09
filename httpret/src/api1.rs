use crate::gather::{gather_get_json_generic, SubRes};
use crate::{response, BodyStream};
use bytes::{BufMut, BytesMut};
use disk::eventchunker::{EventChunkerConf, EventFull};
use err::Error;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use http::{Method, StatusCode};
use hyper::{Body, Client, Request, Response};
use items::{RangeCompletableItem, Sitemty, StreamItem};
use itertools::Itertools;
use netpod::query::RawEventsQuery;
use netpod::{log::*, ByteSize, Channel, FileIoBufferSize, NanoRange, NodeConfigCached, PerfOpts, Shape, APP_OCTET};
use netpod::{ChannelSearchQuery, ChannelSearchResult, ProxyConfig, APP_JSON};
use parse::channelconfig::{
    extract_matching_config_entry, read_local_config, Config, ConfigEntry, MatchingConfigEntry,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
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
                    .search_hosts
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh)) {
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
                let nt = |res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<SubRes<ChannelSearchResult>>| {
                    let mut res = ChannelSearchResultV1(vec![]);
                    for j in all {
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
                                    channels: vec![],
                                    error: None,
                                };
                                res.0.push(u);
                                i2 = res.0.len() - 1;
                            }
                            res.0[i2].channels.push(k.name);
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
                    .search_hosts
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh)) {
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
                let nt = |res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<SubRes<ChannelSearchResult>>| {
                    let mut res = ChannelConfigsResponseV1(vec![]);
                    for j in all {
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
                                    channels: vec![],
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
    let mut a = vec![];
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
            //use snafu::IntoError;
            //Err(Bad{msg:format!("API error")}.into_error(NoneError)).ctxb(SE!(AddPos))
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

pub async fn proxy_distribute_v1(req: Request<Body>) -> Result<Response<Body>, Error> {
    let (mut sink, body) = Body::channel();
    let uri = format!("http://sf-daqbuf-33:8371{}", req.uri().path());
    let res = Response::builder().status(StatusCode::OK).body(body)?;
    tokio::spawn(async move {
        let req = Request::builder().method(Method::GET).uri(uri).body(Body::empty())?;
        let res = Client::new().request(req).await?;
        if res.status() == StatusCode::OK {
            let (_heads, mut body) = res.into_parts();
            loop {
                use hyper::body::HttpBody;
                let chunk = body.data().await;
                if let Some(k) = chunk {
                    let k = k?;
                    sink.send_data(k).await?;
                } else {
                    break;
                }
            }
        }
        Ok::<_, Error>(())
    });
    Ok(res)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Api1Range {
    #[serde(rename = "startDate")]
    start_date: String,
    #[serde(rename = "endDate")]
    end_date: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Api1Query {
    channels: Vec<String>,
    range: Api1Range,
    // The following are unofficial and not-to-be-used parameters:
    #[serde(rename = "fileIoBufferSize", default)]
    file_io_buffer_size: Option<FileIoBufferSize>,
    #[serde(default)]
    decompress: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Api1ChannelHeader {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(rename = "byteOrder")]
    byte_order: String,
    shape: Vec<u32>,
    compression: Option<usize>,
}

pub struct DataApiPython3DataStream {
    range: NanoRange,
    channels: Vec<Channel>,
    node_config: NodeConfigCached,
    chan_ix: usize,
    chan_stream: Option<Pin<Box<dyn Stream<Item = Result<BytesMut, Error>> + Send>>>,
    config_fut: Option<Pin<Box<dyn Future<Output = Result<Config, Error>> + Send>>>,
    file_io_buffer_size: FileIoBufferSize,
    do_decompress: bool,
    data_done: bool,
    completed: bool,
}

impl DataApiPython3DataStream {
    pub fn new(
        range: NanoRange,
        channels: Vec<Channel>,
        file_io_buffer_size: FileIoBufferSize,
        do_decompress: bool,
        node_config: NodeConfigCached,
    ) -> Self {
        Self {
            range,
            channels,
            node_config,
            chan_ix: 0,
            chan_stream: None,
            config_fut: None,
            file_io_buffer_size,
            do_decompress,
            data_done: false,
            completed: false,
        }
    }

    fn convert_item(
        b: EventFull,
        channel: &Channel,
        entry: &ConfigEntry,
        header_out: &mut bool,
        count_events: &mut usize,
    ) -> Result<BytesMut, Error> {
        let mut d = BytesMut::new();
        for i1 in 0..b.tss.len() {
            if *count_events < 6 {
                info!(
                    "deco len {:?}  BE {}  scalar-type {:?}  shape {:?}  comps {:?}",
                    b.decomps[i1].as_ref().map(|x| x.len()),
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
            let compression = if let Some(_) = &b.comps[i1] { Some(1) } else { None };
            if !*header_out {
                let head = Api1ChannelHeader {
                    name: channel.name.clone(),
                    ty: b.scalar_types[i1].to_api3proto().into(),
                    byte_order: if b.be[i1] {
                        "BIG_ENDIAN".into()
                    } else {
                        "LITTLE_ENDIAN".into()
                    },
                    // The shape is inconsistent on the events.
                    // Seems like the config is to be trusted in this case.
                    shape: shape_to_api3proto(&entry.shape),
                    compression,
                };
                let h = serde_json::to_string(&head)?;
                info!("sending channel header {}", h);
                let l1 = 1 + h.as_bytes().len() as u32;
                d.put_u32(l1);
                d.put_u8(0);
                d.extend_from_slice(h.as_bytes());
                d.put_u32(l1);
                *header_out = true;
            }
            {
                match &b.shapes[i1] {
                    Shape::Image(_, _) => {
                        let l1 = 17 + b.blobs[i1].len() as u32;
                        d.put_u32(l1);
                        d.put_u8(1);
                        d.put_u64(b.tss[i1]);
                        d.put_u64(b.pulses[i1]);
                        d.put_slice(&b.blobs[i1]);
                        d.put_u32(l1);
                    }
                    Shape::Wave(_) => {
                        let l1 = 17 + b.blobs[i1].len() as u32;
                        d.put_u32(l1);
                        d.put_u8(1);
                        d.put_u64(b.tss[i1]);
                        d.put_u64(b.pulses[i1]);
                        d.put_slice(&b.blobs[i1]);
                        d.put_u32(l1);
                    }
                    _ => {
                        let l1 = 17 + b.blobs[i1].len() as u32;
                        d.put_u32(l1);
                        d.put_u8(1);
                        d.put_u64(b.tss[i1]);
                        d.put_u64(b.pulses[i1]);
                        d.put_slice(&b.blobs[i1]);
                        d.put_u32(l1);
                    }
                }
            }
            *count_events += 1;
        }
        Ok(d)
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
                        Ready(k) => match k {
                            Some(k) => match k {
                                Ok(k) => Ready(Some(Ok(k))),
                                Err(e) => {
                                    self.data_done = true;
                                    Ready(Some(Err(e)))
                                }
                            },
                            None => {
                                self.chan_stream = None;
                                continue;
                            }
                        },
                        Pending => Pending,
                    }
                } else if let Some(fut) = &mut self.config_fut {
                    match fut.poll_unpin(cx) {
                        Ready(Ok(config)) => {
                            self.config_fut = None;
                            let entry_res = match extract_matching_config_entry(&self.range, &config) {
                                Ok(k) => k,
                                Err(e) => return Err(e)?,
                            };
                            let entry = match entry_res {
                                MatchingConfigEntry::None => return Err(Error::with_msg("no config entry found"))?,
                                MatchingConfigEntry::Multiple => {
                                    return Err(Error::with_msg("multiple config entries found"))?
                                }
                                MatchingConfigEntry::Entry(entry) => entry.clone(),
                            };
                            let channel = self.channels[self.chan_ix - 1].clone();
                            info!("found channel_config for {}: {:?}", channel.name, entry);
                            let evq = RawEventsQuery {
                                channel,
                                range: self.range.clone(),
                                agg_kind: netpod::AggKind::EventBlobs,
                                disk_io_buffer_size: self.file_io_buffer_size.0,
                                do_decompress: self.do_decompress,
                            };
                            let perf_opts = PerfOpts { inmem_bufcap: 1024 * 4 };
                            // TODO is this a good to place decide this?
                            let s = if self.node_config.node_config.cluster.is_central_storage {
                                debug!("Set up central storage stream");
                                // TODO pull up this config
                                let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
                                let s = disk::raw::conn::make_local_event_blobs_stream(
                                    evq.range.clone(),
                                    evq.channel.clone(),
                                    &entry,
                                    evq.agg_kind.need_expand(),
                                    evq.do_decompress,
                                    event_chunker_conf,
                                    self.file_io_buffer_size.clone(),
                                    &self.node_config,
                                )?;
                                Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>
                            } else {
                                debug!("Set up merged remote stream");
                                let s = disk::merge::mergedblobsfromremotes::MergedBlobsFromRemotes::new(
                                    evq,
                                    perf_opts,
                                    self.node_config.node_config.cluster.clone(),
                                );
                                Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>
                            };
                            let s = s.map({
                                let mut header_out = false;
                                let mut count_events = 0;
                                let channel = self.channels[self.chan_ix - 1].clone();
                                move |b| {
                                    let ret = match b {
                                        Ok(b) => {
                                            let f = match b {
                                                StreamItem::DataItem(RangeCompletableItem::Data(b)) => {
                                                    Self::convert_item(
                                                        b,
                                                        &channel,
                                                        &entry,
                                                        &mut header_out,
                                                        &mut count_events,
                                                    )?
                                                }
                                                _ => BytesMut::new(),
                                            };
                                            Ok(f)
                                        }
                                        Err(e) => Err(e),
                                    };
                                    ret
                                }
                            });
                            //let _ = Box::new(s) as Box<dyn Stream<Item = Result<BytesMut, Error>> + Unpin>;
                            self.chan_stream = Some(Box::pin(s));
                            continue;
                        }
                        Ready(Err(e)) => {
                            self.config_fut = None;
                            self.data_done = true;
                            error!("api1_binary_events  error {:?}", e);
                            Ready(Some(Err(Error::with_msg_no_trace("can not parse channel config"))))
                        }
                        Pending => Pending,
                    }
                } else {
                    if self.chan_ix >= self.channels.len() {
                        self.data_done = true;
                        continue;
                    } else {
                        let channel = self.channels[self.chan_ix].clone();
                        self.chan_ix += 1;
                        self.config_fut = Some(Box::pin(read_local_config(
                            channel.clone(),
                            self.node_config.node.clone(),
                        )));
                        continue;
                    }
                }
            };
        }
    }
}

pub async fn api1_binary_events(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    debug!("api1_binary_events  uri: {:?}  headers: {:?}", req.uri(), req.headers());
    let accept_def = "";
    let accept = req
        .headers()
        .get(http::header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def))
        .to_owned();
    let (_head, body) = req.into_parts();
    let body_data = hyper::body::to_bytes(body).await?;
    let qu: Api1Query = if let Ok(qu) = serde_json::from_slice(&body_data) {
        qu
    } else {
        error!("got body_data: {:?}", String::from_utf8(body_data[..].to_vec()));
        return Err(Error::with_msg_no_trace("can not parse query"));
    };
    debug!("got Api1Query: {:?}", qu);
    let beg_date = chrono::DateTime::parse_from_rfc3339(&qu.range.start_date);
    let end_date = chrono::DateTime::parse_from_rfc3339(&qu.range.end_date);
    let beg_date = beg_date?;
    let end_date = end_date?;
    debug!("beg_date {:?}  end_date {:?}", beg_date, end_date);
    //let url = Url::parse(&format!("dummy:{}", req.uri()))?;
    //let query = PlainEventsBinaryQuery::from_url(&url)?;
    // TODO add stricter check for types, check with client.
    if accept == APP_OCTET {}
    if false {
        let e = Error::with_msg(format!("unexpected Accept: {:?}", accept));
        error!("{:?}", e);
        return Err(e);
    }
    let beg_ns = beg_date.timestamp() as u64 * 1000000000 + beg_date.timestamp_subsec_nanos() as u64;
    let end_ns = end_date.timestamp() as u64 * 1000000000 + end_date.timestamp_subsec_nanos() as u64;
    let range = NanoRange {
        beg: beg_ns,
        end: end_ns,
    };
    // TODO use the proper backend name:
    let backend = "DUMMY";
    let chans = qu
        .channels
        .iter()
        .map(|x| Channel {
            backend: backend.into(),
            name: x.clone(),
        })
        .collect();
    let file_io_buffer_size = if let Some(k) = qu.file_io_buffer_size {
        k
    } else {
        node_config.node_config.cluster.file_io_buffer_size.clone()
    };
    let s = DataApiPython3DataStream::new(
        range.clone(),
        chans,
        file_io_buffer_size,
        qu.decompress,
        node_config.clone(),
    );
    let ret = response(StatusCode::OK).header("x-daqbuffer-request-id", "dummy");
    let ret = ret.body(BodyStream::wrapped(s, format!("api1_binary_events")))?;
    return Ok(ret);
}

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
