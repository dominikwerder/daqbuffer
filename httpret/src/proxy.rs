use crate::api1::{channel_search_configs_v1, channel_search_list_v1, gather_json_2_v1, proxy_distribute_v1};
use crate::gather::{gather_get_json_generic, SubRes};
use crate::{response, Cont};
use disk::binned::query::BinnedQuery;
use disk::events::PlainEventsJsonQuery;
use err::Error;
use http::StatusCode;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use itertools::Itertools;
use netpod::log::*;
use netpod::{
    AppendToUrl, ChannelConfigQuery, ChannelSearchQuery, ChannelSearchResult, FromUrl, HasBackend, HasTimeout,
    ProxyConfig, APP_JSON,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
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
        Ok(channel_search_list_v1(req, proxy_config).await?)
    } else if path == "/api/1/channels/config" {
        Ok(channel_search_configs_v1(req, proxy_config).await?)
    } else if path == "/api/1/stats/version" {
        Err(Error::with_msg("todo"))
    } else if path == "/api/1/stats/" {
        Err(Error::with_msg("todo"))
    } else if path.starts_with("/api/1/gather/") {
        Ok(gather_json_2_v1(req, "/api/1/gather/", proxy_config).await?)
    } else if path == "/api/4/backends" {
        Ok(backends(req, proxy_config).await?)
    } else if path == "/api/4/search/channel" {
        Ok(channel_search(req, proxy_config).await?)
    } else if path == "/api/4/events" {
        Ok(proxy_single_backend_query::<PlainEventsJsonQuery>(req, proxy_config).await?)
    } else if path == "/api/4/binned" {
        Ok(proxy_single_backend_query::<BinnedQuery>(req, proxy_config).await?)
    } else if path == "/api/4/channel/config" {
        Ok(proxy_single_backend_query::<ChannelConfigQuery>(req, proxy_config).await?)
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

#[derive(Serialize, Deserialize)]
pub struct BackendsResponse {
    backends: Vec<String>,
}

pub async fn backends(_req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let backends: Vec<_> = proxy_config.backends.iter().map(|k| k.name.to_string()).collect();
    let res = BackendsResponse { backends };
    let ret = response(StatusCode::OK).body(Body::from(serde_json::to_vec(&res)?))?;
    Ok(ret)
}

pub async fn channel_search(req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get(http::header::ACCEPT) {
        Some(v) => {
            if v == APP_JSON {
                let url = Url::parse(&format!("dummy:{}", head.uri))?;
                let query = ChannelSearchQuery::from_url(&url)?;
                let urls = proxy_config
                    .search_hosts
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(_e) => Err(Error::with_msg(format!("parse error for: {:?}", sh))),
                    })
                    .fold_ok(vec![], |mut a, x| {
                        a.push(x);
                        a
                    })?;
                let tags = urls.iter().map(|k| k.to_string()).collect();
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
                        .body(Body::from(serde_json::to_string(&res)?))?;
                    Ok(res)
                };
                let ret =
                    gather_get_json_generic(http::Method::GET, urls, None, tags, nt, ft, Duration::from_millis(3000))
                        .await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

pub async fn proxy_single_backend_query<QT>(
    req: Request<Body>,
    proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error>
where
    QT: FromUrl + HasBackend + AppendToUrl + HasTimeout,
{
    let (head, _body) = req.into_parts();
    match head.headers.get(http::header::ACCEPT) {
        Some(v) => {
            if v == APP_JSON {
                let url = Url::parse(&format!("dummy:{}", head.uri))?;
                let query = QT::from_url(&url)?;
                let sh = get_query_host_for_backend(&query.backend(), proxy_config)?;
                let urls = [sh]
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}{}", sh, head.uri.path())) {
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
                        match serde_json::from_slice::<JsonValue>(&body) {
                            Ok(k) => Ok(k),
                            Err(e) => Err(e.into()),
                        }
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                };
                let ft = |mut all: Vec<SubRes<JsonValue>>| {
                    if all.len() > 0 {
                        all.truncate(1);
                        let z = all.pop().unwrap();
                        let res = z.val;
                        let res = response(StatusCode::OK)
                            .header(http::header::CONTENT_TYPE, APP_JSON)
                            .body(Body::from(serde_json::to_string(&res)?))?;
                        return Ok(res);
                    } else {
                        return Err(Error::with_msg("no response from upstream"));
                    }
                };
                let ret = gather_get_json_generic(http::Method::GET, urls, None, tags, nt, ft, query.timeout()).await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}

fn get_query_host_for_backend(backend: &str, proxy_config: &ProxyConfig) -> Result<String, Error> {
    for back in &proxy_config.backends {
        if back.name == backend {
            return Ok(back.url.clone());
        }
    }
    return Err(Error::with_msg(format!("host not found for backend {:?}", backend)));
}
