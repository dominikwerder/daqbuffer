use crate::response;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use http::{header, Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::log::*;
use netpod::{NodeConfigCached, APP_JSON_LINES};
use serde::Serialize;

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
            use futures_util::StreamExt;
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
            use futures_util::StreamExt;
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
