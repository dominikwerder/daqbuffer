use crate::channelconfig::chconf_from_events_quorum;
use crate::err::Error;
use crate::response;
use crate::response_err;
use crate::ToPublicResponse;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::future;
use futures_util::stream;
use futures_util::StreamExt;
use http::header;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::ACCEPT_ALL;
use netpod::APP_CBOR;
use netpod::APP_JSON;
use nodenet::client::OpenBoxedBytesViaHttp;
use query::api4::events::PlainEventsQuery;
use url::Url;

pub struct EventsHandler {}

impl EventsHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        match plain_events(req, ctx, node_config).await {
            Ok(ret) => Ok(ret),
            Err(e) => {
                error!("EventsHandler sees: {e}");
                Ok(e.to_public_response())
            }
        }
    }
}

async fn plain_events(req: Requ, ctx: &ReqCtx, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
    let accept_def = APP_JSON;
    let accept = req
        .headers()
        .get(header::ACCEPT)
        .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
    let url = req_uri_to_url(req.uri())?;
    if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
        Ok(plain_events_json(url, req, ctx, node_config).await?)
    } else if accept == APP_CBOR {
        Ok(plain_events_cbor(url, req, ctx, node_config).await?)
    } else {
        let ret = response_err(StatusCode::NOT_ACCEPTABLE, format!("unsupported accept: {}", accept))?;
        Ok(ret)
    }
}

async fn plain_events_cbor(url: Url, req: Requ, ctx: &ReqCtx, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
    let evq = PlainEventsQuery::from_url(&url).map_err(|e| e.add_public_msg(format!("Can not understand query")))?;
    let ch_conf = chconf_from_events_quorum(&evq, ctx, ncc)
        .await?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
    info!("plain_events_cbor  chconf_from_events_quorum: {ch_conf:?}  {req:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone());
    let stream = streams::plaineventscbor::plain_events_cbor(&evq, ch_conf, ctx, Box::pin(open_bytes)).await?;
    use future::ready;
    let stream = stream
        .flat_map(|x| match x {
            Ok(y) => {
                use bytes::BufMut;
                let buf = y.into_inner();
                let mut b2 = BytesMut::with_capacity(8);
                b2.put_u32_le(buf.len() as u32);
                stream::iter([Ok::<_, Error>(b2.freeze()), Ok(buf)])
            }
            // TODO handle other cases
            _ => stream::iter([Ok(Bytes::new()), Ok(Bytes::new())]),
        })
        .filter(|x| if let Ok(x) = x { ready(x.len() > 0) } else { ready(true) });
    let ret = response(StatusCode::OK).body(body_stream(stream))?;
    Ok(ret)
}

async fn plain_events_json(
    url: Url,
    req: Requ,
    ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    info!("plain_events_json  req: {:?}", req);
    let (_head, _body) = req.into_parts();
    let query = PlainEventsQuery::from_url(&url)?;
    info!("plain_events_json  query {query:?}");
    // TODO handle None case better and return 404
    let ch_conf = chconf_from_events_quorum(&query, ctx, node_config)
        .await
        .map_err(Error::from)?
        .ok_or_else(|| Error::with_msg_no_trace("channel not found"))?;
    info!("plain_events_json  chconf_from_events_quorum: {ch_conf:?}");
    let open_bytes = OpenBoxedBytesViaHttp::new(node_config.node_config.cluster.clone());
    let item = streams::plaineventsjson::plain_events_json(
        &query,
        ch_conf,
        ctx,
        &node_config.node_config.cluster,
        Box::pin(open_bytes),
    )
    .await;
    let item = match item {
        Ok(item) => item,
        Err(e) => {
            error!("got error from streams::plaineventsjson::plain_events_json {e:?}");
            return Err(e.into());
        }
    };
    let ret = response(StatusCode::OK).body(ToJsonBody::from(&item).into_body())?;
    Ok(ret)
}
