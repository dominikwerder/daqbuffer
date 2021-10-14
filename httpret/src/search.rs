use crate::response;
use err::Error;
use http::header;
use hyper::{Body, Request, Response, StatusCode};
use netpod::{log::*, APP_JSON};
use netpod::{ChannelSearchQuery, NodeConfigCached};
use url::Url;

pub async fn channel_search(req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get(header::ACCEPT) {
        Some(v) if v == APP_JSON => {
            let s1 = format!("dummy:{}", head.uri);
            info!("try to parse {:?}", s1);
            let url = Url::parse(&s1)?;
            let query = ChannelSearchQuery::from_url(&url)?;
            info!("search query: {:?}", query);
            let res = dbconn::search::search_channel(query, node_config).await?;
            let body = Body::from(serde_json::to_string(&res)?);
            let ret = super::response(StatusCode::OK).body(body)?;
            Ok(ret)
        }
        _ => Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?),
    }
}
