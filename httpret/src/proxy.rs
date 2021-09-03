use crate::api1::{channel_search_configs_v1, channel_search_list_v1, gather_json_2_v1, proxy_distribute_v1};
use crate::gather::{gather_get_json_generic, SubRes};
use crate::{api_4_docs, response, Cont};
use disk::binned::query::BinnedQuery;
use disk::events::PlainEventsJsonQuery;
use err::Error;
use futures_core::Stream;
use futures_util::pin_mut;
use http::{Method, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use itertools::Itertools;
use netpod::log::*;
use netpod::{
    AppendToUrl, ChannelConfigQuery, ChannelSearchQuery, ChannelSearchResult, ChannelSearchSingleResult, FromUrl,
    HasBackend, HasTimeout, ProxyConfig, APP_JSON,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncRead, ReadBuf};
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
    let distri_pre = "/distri/";
    if path == "/api/1/channels" {
        Ok(channel_search_list_v1(req, proxy_config).await?)
    } else if path == "/api/1/channels/config" {
        Ok(channel_search_configs_v1(req, proxy_config).await?)
    } else if path == "/api/1/stats/version" {
        Err(Error::with_msg("todo"))
    } else if path == "/api/1/stats/" {
        Err(Error::with_msg("todo"))
    } else if path == "/api/1/query" {
        Ok(proxy_api1_single_backend_query(req, proxy_config).await?)
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
    } else if path.starts_with("/api/1/documentation/") {
        if req.method() == Method::GET {
            Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with("/api/4/documentation/") {
        if req.method() == Method::GET {
            api_4_docs(path)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else if path.starts_with(distri_pre)
        && path
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || ['/', '.', '-', '_'].contains(&c))
        && !path.contains("..")
    {
        if req.method() == Method::GET {
            let s = FileStream {
                file: File::open(format!("/opt/distri/{}", &path[distri_pre.len()..])).await?,
            };
            Ok(response(StatusCode::OK).body(Body::wrap_stream(s))?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    } else {
        Ok(response(StatusCode::NOT_FOUND).body(Body::from(format!(
            "Sorry, not found: {:?}  {:?}  {:?}",
            req.method(),
            req.uri().path(),
            req.uri().query(),
        )))?)
    }
}

pub struct FileStream {
    file: File,
}

impl Stream for FileStream {
    type Item = Result<Vec<u8>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut buf = vec![0; 1024 * 8];
        let mut rb = ReadBuf::new(&mut buf);
        let f = &mut self.file;
        pin_mut!(f);
        match f.poll_read(cx, &mut rb) {
            Ready(k) => match k {
                Ok(_) => {
                    let n = rb.filled().len();
                    if n == 0 {
                        Ready(None)
                    } else {
                        buf.truncate(n);
                        Ready(Some(Ok(buf)))
                    }
                }
                Err(e) => Ready(Some(Err(e.into()))),
            },
            Pending => Pending,
        }
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
                let mut bodies = vec![];
                let mut urls = proxy_config
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
                        bodies.push(None);
                        a
                    })?;
                if let (Some(hosts), Some(backends)) =
                    (&proxy_config.api_0_search_hosts, &proxy_config.api_0_search_backends)
                {
                    #[derive(Serialize)]
                    struct QueryApi0 {
                        backends: Vec<String>,
                        regex: String,
                        #[serde(rename = "sourceRegex")]
                        source_regex: String,
                        ordering: String,
                        reload: bool,
                    }
                    hosts.iter().zip(backends.iter()).for_each(|(sh, back)| {
                        let url = Url::parse(&format!("{}/channels/config", sh)).unwrap();
                        urls.push(url);
                        let q = QueryApi0 {
                            backends: vec![back.into()],
                            ordering: "asc".into(),
                            reload: false,
                            regex: query.name_regex.clone(),
                            source_regex: query.source_regex.clone(),
                        };
                        let qs = serde_json::to_string(&q).unwrap();
                        bodies.push(Some(Body::from(qs)));
                    });
                }
                let tags = urls.iter().map(|k| k.to_string()).collect();
                let nt = |res| {
                    let fut = async {
                        let body = hyper::body::to_bytes(res).await?;
                        //info!("got a result {:?}", body);
                        let res: ChannelSearchResult = match serde_json::from_slice::<ChannelSearchResult>(&body) {
                            Ok(k) => k,
                            Err(_) => {
                                #[derive(Deserialize)]
                                struct ResItemApi0 {
                                    name: String,
                                    source: String,
                                    backend: String,
                                    #[serde(rename = "type")]
                                    ty: String,
                                }
                                #[derive(Deserialize)]
                                struct ResContApi0 {
                                    #[allow(dead_code)]
                                    backend: String,
                                    channels: Vec<ResItemApi0>,
                                }
                                match serde_json::from_slice::<Vec<ResContApi0>>(&body) {
                                    Ok(k) => {
                                        let mut a = vec![];
                                        if let Some(g) = k.first() {
                                            for c in &g.channels {
                                                let z = ChannelSearchSingleResult {
                                                    backend: c.backend.clone(),
                                                    description: String::new(),
                                                    name: c.name.clone(),
                                                    shape: vec![],
                                                    source: c.source.clone(),
                                                    ty: c.ty.clone(),
                                                    unit: String::new(),
                                                    is_api_0: Some(true),
                                                };
                                                a.push(z);
                                            }
                                        }
                                        let ret = ChannelSearchResult { channels: a };
                                        ret
                                    }
                                    Err(_) => {
                                        error!("Channel search response parse failed");
                                        ChannelSearchResult { channels: vec![] }
                                    }
                                }
                            }
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

pub async fn proxy_api1_single_backend_query(
    req: Request<Body>,
    proxy_config: &ProxyConfig,
) -> Result<Response<Body>, Error> {
    panic!()
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
                let bodies = (0..urls.len()).into_iter().map(|_| None).collect();
                let ret =
                    gather_get_json_generic(http::Method::GET, urls, bodies, tags, nt, ft, query.timeout()).await?;
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
