use crate::response;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use http::{header, Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::{get_url_query_pairs, Channel, NanoRange};
use netpod::{NodeConfigCached, APP_JSON_LINES};
use serde::Serialize;
use serde_json::Value as JsVal;
use url::Url;

fn json_lines_stream<S, I>(stream: S) -> impl Stream<Item = Result<Vec<u8>, Error>>
where
    S: Stream<Item = Result<I, Error>>,
    I: Serialize,
{
    stream.map(|k| {
        k.map(|k| {
            let mut a = serde_json::to_vec(&k).unwrap();
            a.push(0xa);
            a
        })
    })
}

pub struct ListIndexFilesHttpFunction {}

impl ListIndexFilesHttpFunction {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/list/indexfiles"
    }

    pub fn name() -> &'static str {
        "ListIndexFilesHttpFunction"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;
        let s = archapp_wrap::archapp::archeng::indexfiles::list_index_files(conf);
        let s = futures_util::stream::unfold(s, |mut st| async move {
            let x = st.next().await;
            match x {
                Some(x) => match x {
                    Ok(x) => {
                        let mut x = serde_json::to_vec(&x).unwrap();
                        x.push(b'\n');
                        Some((Ok::<_, Error>(x), st))
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        None
                    }
                },
                None => None,
            }
        });
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(s))?)
    }
}

pub struct ScanIndexFiles {}

impl ScanIndexFiles {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/scan/indexfiles"
    }

    pub fn name() -> &'static str {
        "ScanIndexFiles"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;
        let s = archapp_wrap::archapp::archeng::indexfiles::scan_index_files(conf.clone());
        let s = json_lines_stream(s);
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(s))?)
    }
}

pub struct ScanChannels {}

impl ScanChannels {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/scan/channels"
    }

    pub fn name() -> &'static str {
        "ScanChannels"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;
        let s = archapp_wrap::archapp::archeng::indexfiles::scan_channels(conf.clone());
        let s = json_lines_stream(s);
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(s))?)
    }
}

pub struct ChannelNames {}

impl ChannelNames {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/channel/names"
    }

    pub fn name() -> &'static str {
        "ChannelNames"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;
        use archapp_wrap::archapp::archeng;
        let stream = archeng::configs::ChannelNameStream::new(conf.database.clone());
        let stream = json_lines_stream(stream);
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(stream))?)
    }
}

pub struct ScanConfigs {}

impl ScanConfigs {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/scan/configs"
    }

    pub fn name() -> &'static str {
        "ScanConfigs"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;
        use archapp_wrap::archapp::archeng;
        let stream = archeng::configs::ChannelNameStream::new(conf.database.clone());
        let stream = archeng::configs::ConfigStream::new(stream, conf.clone());
        let stream = json_lines_stream(stream);
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(stream))?)
    }
}

pub struct BlockRefStream {}

impl BlockRefStream {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/blockrefstream"
    }

    pub fn name() -> &'static str {
        "BlockRefStream"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;
        let range = NanoRange { beg: 0, end: u64::MAX };
        let url = Url::parse(&format!("dummy:{}", req.uri()))?;
        let pairs = get_url_query_pairs(&url);
        let channel_name = pairs.get("channelName").map(String::from).unwrap_or("NONE".into());
        let channel = Channel {
            backend: "".into(),
            name: channel_name,
            //name: "ARIDI-PCT:CURRENT".into(),
        };
        use archapp_wrap::archapp::archeng;
        let ixpaths = archeng::indexfiles::index_file_path_list(channel.clone(), conf.database.clone()).await?;
        info!("got categorized ixpaths: {:?}", ixpaths);
        let ixpath = ixpaths.first().unwrap().clone();
        let s = archeng::blockrefstream::blockref_stream(channel, range, true, ixpath);
        let s = s.map(|item| match item {
            Ok(item) => {
                use archeng::blockrefstream::BlockrefItem::*;
                match item {
                    Blockref(_k, jsval) => Ok(jsval),
                    JsVal(jsval) => Ok(jsval),
                }
            }
            Err(e) => Err(e),
        });
        let s = json_lines_stream(s);
        let s = s.map(|item| match item {
            Ok(k) => Ok(k),
            Err(e) => {
                error!("observe error: {}", e);
                Err(e)
            }
        });
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(s))?)
    }
}

pub struct BlockStream {}

impl BlockStream {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/blockstream"
    }

    pub fn name() -> &'static str {
        "BlockStream"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;
        let range = NanoRange { beg: 0, end: u64::MAX };
        let url = Url::parse(&format!("dummy:{}", req.uri()))?;
        let pairs = get_url_query_pairs(&url);
        let read_queue = pairs.get("readQueue").unwrap_or(&"1".to_string()).parse()?;
        let channel_name = pairs.get("channelName").map(String::from).unwrap_or("NONE".into());
        let channel = Channel {
            backend: node_config.node.backend.clone(),
            name: channel_name,
        };
        use archapp_wrap::archapp::archeng;
        let ixpaths = archeng::indexfiles::index_file_path_list(channel.clone(), conf.database.clone()).await?;
        info!("got categorized ixpaths: {:?}", ixpaths);
        let ixpath = ixpaths.first().unwrap().clone();
        let s = archeng::blockrefstream::blockref_stream(channel, range.clone(), true, ixpath);
        let s = Box::pin(s);
        let s = archeng::blockstream::BlockStream::new(s, range.clone(), read_queue);
        let s = s.map(|item| match item {
            Ok(item) => {
                use archeng::blockstream::BlockItem;
                match item {
                    BlockItem::EventsItem(_item) => Ok(JsVal::String("EventsItem".into())),
                    BlockItem::JsVal(jsval) => Ok(jsval),
                }
            }
            Err(e) => Err(e),
        });
        let s = json_lines_stream(s);
        let s = s.map(|item| match item {
            Ok(k) => Ok(k),
            Err(e) => {
                error!("observe error: {}", e);
                Err(e)
            }
        });
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(s))?)
    }
}

pub struct ListChannelsHttpFunction {}

impl ListChannelsHttpFunction {
    pub fn prefix() -> &'static str {
        "/api/4/channelarchiver/list/channels"
    }

    pub fn name() -> &'static str {
        "ListChannelsHttpFunction"
    }

    pub fn should_handle(path: &str) -> Option<Self> {
        if path.starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        info!("{}  handle  uri: {:?}", Self::name(), req.uri());
        let conf = node_config
            .node
            .channel_archiver
            .as_ref()
            .ok_or(Error::with_msg_no_trace(
                "this node is not configured as channel archiver",
            ))?;

        let s = archapp_wrap::archapp::archeng::list_all_channels(conf);
        let s = futures_util::stream::unfold(s, |mut st| async move {
            let x = st.next().await;
            match x {
                Some(x) => match x {
                    Ok(x) => {
                        let mut x = serde_json::to_vec(&x).unwrap();
                        x.push(b'\n');
                        Some((Ok::<_, Error>(x), st))
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        None
                    }
                },
                None => None,
            }
        });
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_LINES)
            .body(Body::wrap_stream(s))?)
    }
}
