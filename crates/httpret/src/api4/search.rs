use crate::bodystream::response;
use crate::bodystream::ToPublicResponse;
use crate::err::Error;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;

pub async fn channel_search(req: Requ, node_config: &NodeConfigCached) -> Result<ChannelSearchResult, Error> {
    let url = req_uri_to_url(req.uri())?;
    let query = ChannelSearchQuery::from_url(&url)?;
    info!("search query: {:?}", query);
    let res = dbconn::search::search_channel(query, node_config).await?;
    Ok(res)
}

pub struct ChannelSearchHandler {}

impl ChannelSearchHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/search/channel" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                match channel_search(req, node_config).await {
                    Ok(item) => Ok(response(StatusCode::OK).body(ToJsonBody::from(&item).into_body())?),
                    Err(e) => {
                        warn!("handle: got error from channel_search: {e:?}");
                        Ok(e.to_public_response())
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }
}
