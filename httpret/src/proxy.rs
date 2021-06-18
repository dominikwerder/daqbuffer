use crate::api1::{channels_config_v1, channels_list_v1, gather_json_2_v1, gather_json_v1, proxy_distribute_v1};
use crate::gather::gather_get_json_generic;
use crate::{proxy_mark, response, Cont};
use err::Error;
use http::{HeaderValue, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use netpod::log::*;
use netpod::{ChannelSearchQuery, ChannelSearchResult, ProxyConfig};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;
use url::Url;

pub async fn proxy(proxy_config: ProxyConfig) -> Result<(), Error> {
    use std::str::FromStr;
    let addr = SocketAddr::from_str(&format!("{}:{}", proxy_config.listen, proxy_config.port))?;
    let make_service = make_service_fn({
        move |_conn| {
            let proxy_config = proxy_config.clone();
            async move {
                Ok::<_, Error>(service_fn({
                    move |req| {
                        let f = proxy_http_service(req, proxy_config.clone());
                        Cont { f: Box::pin(f) }
                    }
                }))
            }
        }
    });
    Server::bind(&addr).serve(make_service).await?;
    Ok(())
}

async fn proxy_http_service(req: Request<Body>, proxy_config: ProxyConfig) -> Result<Response<Body>, Error> {
    match proxy_http_service_try(req, &proxy_config).await {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("data_api_proxy sees error: {:?}", e);
            Err(e)
        }
    }
}

async fn proxy_http_service_try(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/1/channels" {
        Ok(channels_list_v1(req, proxy_config).await?)
    } else if path == "/api/1/channels/config" {
        Ok(channels_config_v1(req, proxy_config).await?)
    } else if path == "/api/1/stats/version" {
        Ok(gather_json_v1(req, "/stats/version").await?)
    } else if path.starts_with("/api/1/stats/") {
        Ok(gather_json_v1(req, path).await?)
    } else if path.starts_with("/api/1/gather/") {
        Ok(gather_json_2_v1(req, "/api/1/gather/", proxy_config).await?)
    } else if path.starts_with("/api/4/search/channel") {
        Ok(channel_search(req, proxy_config).await?)
    } else if path.starts_with("/distribute") {
        proxy_distribute_v1(req).await
    } else {
        Ok(response(StatusCode::NOT_FOUND).body(Body::from(format!(
            "Sorry, not found: {:?}  {:?}  {:?}",
            req.method(),
            req.uri().path(),
            req.uri().query(),
        )))?)
    }
}

pub async fn channel_search(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get("accept") {
        Some(v) => {
            if v == "application/json" {
                let query = ChannelSearchQuery::from_query_string(head.uri.query())?;
                let urls: Vec<Result<Url, Error>> = proxy_config
                    .search_hosts
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(_e) => Err(Error::with_msg(format!("parse error for: {:?}", sh))),
                    })
                    .collect();
                for u in &urls {
                    match u {
                        Ok(url) => {
                            info!("URL: {}", url.as_str());
                        }
                        Err(_) => {
                            return Err(Error::with_msg("url parse error"));
                        }
                    }
                }
                let urls: Vec<_> = urls.into_iter().map(Result::unwrap).collect();
                let nt = |res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        info!("got a result {:?}", body);
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<ChannelSearchResult>| {
                    let mut res = vec![];
                    for j in all {
                        for k in j.channels {
                            res.push(k);
                        }
                    }
                    let res = ChannelSearchResult { channels: res };
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                let mut ret =
                    gather_get_json_generic(http::Method::GET, urls, nt, ft, Duration::from_millis(3000)).await?;
                ret.headers_mut()
                    .append("x-proxy-log-mark", HeaderValue::from_str(proxy_mark())?);
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

pub async fn events(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get("accept") {
        Some(v) => {
            if v == "application/json" {
                Url::parse(&format!("{}", head.uri))?;
                let query = ChannelSearchQuery::from_query_string(head.uri.query())?;
                let urls: Vec<Result<Url, Error>> = proxy_config
                    .search_hosts
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(_e) => Err(Error::with_msg(format!("parse error for: {:?}", sh))),
                    })
                    .collect();
                for u in &urls {
                    match u {
                        Ok(url) => {
                            info!("URL: {}", url.as_str());
                        }
                        Err(_) => {
                            return Err(Error::with_msg("url parse error"));
                        }
                    }
                }
                let urls: Vec<_> = urls.into_iter().map(Result::unwrap).collect();
                let nt = |res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        info!("got a result {:?}", body);
                        let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                            Ok(k) => k,
                            Err(_) => ChannelSearchResult { channels: vec![] },
                        };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |all: Vec<ChannelSearchResult>| {
                    let mut res = vec![];
                    for j in all {
                        for k in j.channels {
                            res.push(k);
                        }
                    }
                    let res = ChannelSearchResult { channels: res };
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                let mut ret =
                    gather_get_json_generic(http::Method::GET, urls, nt, ft, Duration::from_millis(3000)).await?;
                ret.headers_mut()
                    .append("x-proxy-log-mark", HeaderValue::from_str(proxy_mark())?);
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}
