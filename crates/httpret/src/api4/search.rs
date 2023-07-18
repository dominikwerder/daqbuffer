use crate::bodystream::response;
use crate::bodystream::ToPublicResponse;
use crate::err::Error;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use url::Url;

pub async fn channel_search(req: Request<Body>, node_config: &NodeConfigCached) -> Result<ChannelSearchResult, Error> {
    let url = Url::parse(&format!("dummy://{}", req.uri()))?;
    let query = ChannelSearchQuery::from_url(&url)?;
    info!("search query: {:?}", query);
    let res = dbconn::search::search_channel(query, node_config).await?;
    Ok(res)
}

pub struct ChannelSearchHandler {}

impl ChannelSearchHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/4/search/channel" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
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
                        warn!("handle: got error from channel_search: {e:?}");
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
