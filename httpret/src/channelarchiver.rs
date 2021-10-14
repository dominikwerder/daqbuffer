use crate::response;
use err::Error;
use http::{header, Method, Request, Response, StatusCode};
use hyper::Body;
use netpod::{log::*, NodeConfigCached, APP_JSON_LINES};

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
        let s = archapp_wrap::archapp::archeng::list_index_files(conf);
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
                        //Some((Err(e), st))
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
