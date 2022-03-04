use crate::err::Error;
use crate::response;
use http::{Method, StatusCode};
use hyper::{Body, Request, Response};
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::{ACCEPT_ALL, APP_JSON};

pub struct SettingsThreadsMaxHandler {}

impl SettingsThreadsMaxHandler {
    pub fn path_prefix() -> &'static str {
        "/api/4/settings/read3/threads_max"
    }

    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn put(&self, req: Request<Body>, _node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        let (head, body) = req.into_parts();
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
        let body = hyper::body::to_bytes(body).await?;
        //let threads_max: usize = head.uri.path()[Self::path_prefix().len()..].parse()?;
        let threads_max: usize = String::from_utf8_lossy(&body).parse()?;
        info!("threads_max {threads_max}");
        disk::read3::Read3::get().set_threads_max(threads_max);
        let ret = response(StatusCode::NO_CONTENT).body(Body::empty())?;
        Ok(ret)
    }

    pub async fn get(&self, _req: Request<Body>, _node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        let threads_max = disk::read3::Read3::get().threads_max();
        let ret = response(StatusCode::OK).body(Body::from(format!("{threads_max}")))?;
        Ok(ret)
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() == Method::GET {
            self.get(req, node_config).await
        } else if req.method() == Method::PUT {
            self.put(req, node_config).await
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}
