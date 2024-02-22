use crate::bodystream::response;
use crate::bodystream::response_err_msg;
use crate::err::Error;
use crate::proxy::get_query_host_for_backend;
use crate::requests::accepts_cbor_frames;
use crate::requests::accepts_json_or_all;
use crate::ReqCtx;
use http::header;
use http::Method;
use http::Request;
use http::StatusCode;
use http::Uri;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::connect_client;
use httpclient::Requ;
use httpclient::StreamIncoming;
use httpclient::StreamResponse;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::ProxyConfig;
use netpod::APP_CBOR;
use query::api4::events::PlainEventsQuery;

pub struct EventsHandler {}

impl EventsHandler {
    pub fn path() -> &'static str {
        "/api/4/events"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == Self::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        } else {
            if accepts_cbor_frames(req.headers()) {
                // self.handle_cbor(req, ctx, proxy_config).await
                // Ok(crate::proxy::proxy_backend_query::<PlainEventsQuery>(req, ctx, proxy_config).await?)
                warn!("TODO enabe cbor endpoint");
                Ok(response_err_msg(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("cbor endpoint currently disabled"),
                )?)
            } else if accepts_json_or_all(req.headers()) {
                Ok(crate::proxy::proxy_backend_query::<PlainEventsQuery>(req, ctx, proxy_config).await?)
            } else {
                Err(Error::with_msg_no_trace(format!("bad accept {:?}", req.headers())))
            }
        }
    }

    async fn handle_cbor(&self, req: Requ, ctx: &ReqCtx, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        let (head, _body) = req.into_parts();
        let url = req_uri_to_url(&head.uri)?;
        let pairs = get_url_query_pairs(&url);
        let evq = PlainEventsQuery::from_pairs(&pairs)?;
        info!("{evq:?}");
        // Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)

        // get_random_query_host_for_backend
        let query_host = get_query_host_for_backend(evq.backend(), proxy_config)?;

        // TODO this ignores the fragment
        let url_str = format!("{}{}", query_host, head.uri.path_and_query().unwrap());
        info!("try to contact {url_str}");

        let uri: Uri = url_str.parse()?;
        let req = Request::builder()
            .method(Method::GET)
            .header(header::HOST, uri.host().unwrap())
            .header(header::ACCEPT, APP_CBOR)
            .header(ctx.header_name(), ctx.header_value())
            .uri(&uri)
            .body(body_empty())?;
        let mut client = connect_client(&uri).await?;
        let res = client.send_request(req).await?;
        let (head, body) = res.into_parts();
        if head.status != StatusCode::OK {
            error!("backend returned error: {head:?}");
            Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
        } else {
            info!("backend returned OK");
            Ok(response(StatusCode::OK).body(body_stream(StreamIncoming::new(body)))?)
        }
    }
}
