use err::{Error, PublicError};
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
    let res = client
        .request(req)
        .await
        .map_err(|e| Error::with_msg(format!("get_channel_config request error: {e:?}")))?;
    if res.status().is_success() {
        let buf = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(|e| Error::with_msg(format!("can not read response: {e:?}")))?;
        let ret: ChannelConfigResponse = serde_json::from_slice(&buf)
            .map_err(|e| Error::with_msg(format!("can not parse the channel config response json: {e:?}")))?;
        Ok(ret)
    } else {
        let buf = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(|e| Error::with_msg(format!("can not read response: {e:?}")))?;
        match serde_json::from_slice::<PublicError>(&buf) {
            Ok(e) => Err(e.into()),
            Err(_) => Err(Error::with_msg(format!(
                "can not parse the http error body: {:?}",
                String::from_utf8_lossy(&buf)
            ))),
        }
    }
}
