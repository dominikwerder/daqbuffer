use crate::err::Error;
use crate::gather::{gather_get_json_generic, SubRes, Tag};
use crate::{response, ReqCtx};
use futures_util::Future;
use http::{header, Request, Response, StatusCode};
use hyper::Body;
use itertools::Itertools;
use netpod::log::*;
use netpod::NodeStatus;
use netpod::ACCEPT_ALL;
use netpod::{ChannelSearchQuery, ChannelSearchResult, ProxyConfig, APP_JSON};
use serde_json::Value as JsVal;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::time::Duration;
use url::Url;

pub async fn channel_search(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let vdef = header::HeaderValue::from_static(APP_JSON);
    let v = head.headers.get(header::ACCEPT).unwrap_or(&vdef);
    if v == APP_JSON || v == ACCEPT_ALL {
        let inpurl = Url::parse(&format!("dummy:{}", head.uri))?;
        let query = ChannelSearchQuery::from_url(&inpurl)?;
        let mut bodies = vec![];
        let urls = proxy_config
            .backends
            .iter()
            .filter(|k| {
                if let Some(back) = &query.backend {
                    back == &k.name
                } else {
                    true
                }
            })
            .map(|pb| match Url::parse(&format!("{}/api/4/search/channel", pb.url)) {
                Ok(mut url) => {
                    query.append_to_url(&mut url);
                    Ok(url)
                }
                Err(_) => Err(Error::with_msg(format!("parse error for: {:?}", pb))),
            })
            .fold_ok(vec![], |mut a, x| {
                a.push(x);
                bodies.push(None);
                a
            })?;
        let tags = urls.iter().map(|k| k.to_string()).collect();
        let nt = |tag, res| {
            let fut = async {
                let body = hyper::body::to_bytes(res).await?;
                //info!("got a result {:?}", body);
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
            let res = response(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, APP_JSON)
                .body(Body::from(serde_json::to_string(&res)?))
                .map_err(Error::from)?;
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
    } else {
        Ok(response(StatusCode::NOT_ACCEPTABLE)
            .body(Body::from(format!("{:?}", proxy_config.name)))
            .map_err(Error::from)?)
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
            let mut subs = BTreeMap::new();
            for (tag, sr) in all {
                match sr {
                    Ok(sr) => {
                        let s: Result<NodeStatus, _> = serde_json::from_value(sr.val).map_err(err::Error::from);
                        subs.insert(tag.0, s);
                    }
                    Err(e) => {
                        subs.insert(tag.0, Err(err::Error::from(e)));
                    }
                }
            }
            let ret = NodeStatus {
                name: format!("{}:{}", proxy_config.name, proxy_config.port),
                is_sf_databuffer: false,
                is_archiver_engine: false,
                is_archiver_appliance: false,
                database_size: None,
                table_sizes: None,
                archiver_appliance_status: None,
                subs: Some(subs),
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
