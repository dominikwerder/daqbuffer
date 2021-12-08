use err::Error;
use hyper::{Body, Method};
use netpod::{AppendToUrl, ChannelConfigQuery, ChannelConfigResponse, NodeConfigCached};
use url::Url;

pub async fn get_channel_config(
    q: &ChannelConfigQuery,
    node_config: &NodeConfigCached,
) -> Result<ChannelConfigResponse, Error> {
    let mut url = Url::parse(&format!(
        "http://{}:{}/api/4/channel/config",
        node_config.node.host, node_config.node.port
    ))?;
    q.append_to_url(&mut url);
    let req = hyper::Request::builder()
        .method(Method::GET)
        .uri(url.as_str())
        .body(Body::empty())
        .map_err(Error::from_string)?;
    let client = hyper::Client::new();
    let res = client.request(req).await.map_err(Error::from_string)?;
    if !res.status().is_success() {
        return Err(Error::with_msg("http client error"));
    }
    let buf = hyper::body::to_bytes(res.into_body())
        .await
        .map_err(Error::from_string)?;
    let ret: ChannelConfigResponse = serde_json::from_slice(&buf)?;
    Ok(ret)
}
