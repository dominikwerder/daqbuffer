use crate::bodystream::ToPublicResponse;
use crate::err::Error;
use crate::gather::gather_get_json_generic;
use crate::gather::SubRes;
use crate::gather::Tag;
use crate::response;
use crate::ReqCtx;
use futures_util::Future;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::NodeStatus;
use netpod::NodeStatusSub;
use netpod::ProxyConfig;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use serde_json::Value as JsVal;
use std::collections::VecDeque;
use std::pin::Pin;
use std::time::Duration;
use url::Url;

// TODO model channel search according to StatusNodesRecursive.
// Make sure that backend handling is correct:
// The aggregator asks all backends, except if the user specifies some backend
// in which case it should only go to the matching backends.
// The aggregators and leaf nodes behind should as well not depend on backend,
// but simply answer all matching.

pub async fn channel_search(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<ChannelSearchResult, Error> {
    let (head, _body) = req.into_parts();
    let inpurl = Url::parse(&format!("dummy:{}", head.uri))?;
    let query = ChannelSearchQuery::from_url(&inpurl)?;
    let mut urls = Vec::new();
    let mut tags = Vec::new();
    let mut bodies = Vec::new();
    for pb in &proxy_config.backends {
        if if let Some(b) = &query.backend {
            pb.name.contains(b)
        } else {
            true
        } {
            match Url::parse(&format!("{}/api/4/search/channel", pb.url)) {
                Ok(mut url) => {
                    query.append_to_url(&mut url);
                    tags.push(url.to_string());
                    bodies.push(None);
                    urls.push(url);
                }
                Err(_) => return Err(Error::with_msg(format!("parse error for: {:?}", pb))),
            }
        }
    }
    let nt = |tag, res| {
        let fut = async {
            let body = hyper::body::to_bytes(res).await?;
            info!("got a result {:?}", body);
            let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                Ok(k) => k,
                Err(_) => {
                    let msg = format!("can not parse result: {}", String::from_utf8_lossy(&body));
                    error!("{}", msg);
                    return Err(Error::with_msg_no_trace(msg));
                }
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
    let ft = |all: Vec<(Tag, Result<SubRes<ChannelSearchResult>, Error>)>| {
        let mut res = Vec::new();
        for (_tag, j) in all {
            match j {
                Ok(j) => {
                    for k in j.val.channels {
                        res.push(k);
                    }
                }
                Err(e) => {
                    warn!("{e}");
                }
            }
        }
        let res = ChannelSearchResult { channels: res };
        Ok(res)
    };
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
}

pub struct ChannelSearchAggHandler {}

impl ChannelSearchAggHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/search/channel" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &ProxyConfig) -> Result<Response<Body>, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                match channel_search(req, node_config).await {
                    Ok(item) => {
                        let buf = serde_json::to_vec(&item)?;
                        Ok(response(StatusCode::OK).body(Body::from(buf))?)
                    }
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

pub struct StatusNodesRecursive {}

impl StatusNodesRecursive {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == crate::api4::status::StatusNodesRecursive::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Request<Body>,
        ctx: &ReqCtx,
        node_config: &ProxyConfig,
    ) -> Result<Response<Body>, Error> {
        match self.status(req, ctx, node_config).await {
            Ok(status) => {
                let body = serde_json::to_vec(&status)?;
                let ret = response(StatusCode::OK).body(Body::from(body))?;
                Ok(ret)
            }
            Err(e) => {
                error!("{e}");
                let ret = crate::bodystream::ToPublicResponse::to_public_response(&e);
                Ok(ret)
            }
        }
    }

    async fn status(
        &self,
        _req: Request<Body>,
        _ctx: &ReqCtx,
        proxy_config: &ProxyConfig,
    ) -> Result<NodeStatus, Error> {
        let path = crate::api4::status::StatusNodesRecursive::path();
        let mut bodies = Vec::new();
        let mut urls = Vec::new();
        let mut tags = Vec::new();
        for backend in &proxy_config.backends {
            match Url::parse(&format!("{}{}", backend.url, path)) {
                Ok(url) => {
                    bodies.push(None);
                    tags.push(url.to_string());
                    urls.push(url);
                }
                Err(e) => return Err(Error::with_msg_no_trace(format!("parse error for: {backend:?}  {e:?}"))),
            }
        }
        let nt = |tag, res| {
            let fut = async {
                let body = hyper::body::to_bytes(res).await?;
                let res: JsVal = match serde_json::from_slice(&body) {
                    Ok(k) => k,
                    Err(e) => {
                        error!("{e}");
                        let msg = format!(
                            "gather sub responses, can not parse result: {}  {}",
                            String::from_utf8_lossy(&body),
                            e,
                        );
                        error!("{}", msg);
                        return Err(Error::with_msg_no_trace(msg));
                    }
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
        let ft = |all: Vec<(Tag, Result<SubRes<JsVal>, Error>)>| {
            let mut subs = VecDeque::new();
            for (tag, sr) in all {
                match sr {
                    Ok(sr) => {
                        let s: Result<NodeStatus, _> = serde_json::from_value(sr.val).map_err(err::Error::from);
                        let sub = NodeStatusSub { url: tag.0, status: s };
                        subs.push_back(sub);
                    }
                    Err(e) => {
                        let sub = NodeStatusSub {
                            url: tag.0,
                            status: Err(err::Error::from(e)),
                        };
                        subs.push_back(sub);
                    }
                }
            }
            let ret = NodeStatus {
                name: format!("{}:{}", proxy_config.name, proxy_config.port),
                version: core::env!("CARGO_PKG_VERSION").into(),
                is_sf_databuffer: false,
                is_archiver_engine: false,
                is_archiver_appliance: false,
                database_size: None,
                table_sizes: None,
                archiver_appliance_status: None,
                subs,
            };
            Ok(ret)
        };
        let ret = gather_get_json_generic(
            http::Method::GET,
            urls,
            bodies,
            tags,
            nt,
            ft,
            Duration::from_millis(1200),
        )
        .await?;
        Ok(ret)
    }
}
