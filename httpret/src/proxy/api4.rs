use crate::err::Error;
use crate::gather::{gather_get_json_generic, SubRes};
use crate::response;
use futures_core::Future;
use http::{header, Request, Response, StatusCode};
use hyper::Body;
use itertools::Itertools;
use netpod::log::*;
use netpod::ACCEPT_ALL;
use netpod::{ChannelSearchQuery, ChannelSearchResult, ProxyConfig, APP_JSON};
use serde_json::Value as JsVal;
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
            .backends_search
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
        let ft = |all: Vec<SubRes<ChannelSearchResult>>| {
            let mut res = vec![];
            for j in all {
                for k in j.val.channels {
                    res.push(k);
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

pub async fn node_status(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    let vdef = header::HeaderValue::from_static(APP_JSON);
    let v = head.headers.get(header::ACCEPT).unwrap_or(&vdef);
    if v == APP_JSON || v == ACCEPT_ALL {
        let inpurl = Url::parse(&format!("dummy:{}", head.uri))?;
        let query = ChannelSearchQuery::from_url(&inpurl)?;
        let mut bodies = vec![];
        let urls = proxy_config
            .backends_status
            .iter()
            .filter(|k| {
                if let Some(back) = &query.backend {
                    back == &k.name
                } else {
                    true
                }
            })
            .map(|pb| match Url::parse(&format!("{}/api/4/node_status", pb.url)) {
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
                let res: JsVal = match serde_json::from_slice(&body) {
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
        let ft = |all: Vec<SubRes<JsVal>>| {
            let mut res = vec![];
            for j in all {
                let val = serde_json::json!({
                    j.tag: j.val,
                });
                res.push(val);
            }
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
