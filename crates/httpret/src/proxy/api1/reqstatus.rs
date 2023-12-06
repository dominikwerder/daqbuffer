use crate::bodystream::response;
use crate::err::Error;
use http::header;
use http::Method;
use http::Request;
use http::StatusCode;
use http::Uri;
use httpclient::body_bytes;
use httpclient::body_empty;
use httpclient::connect_client;
use httpclient::read_body_bytes;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::log::*;
use netpod::ProxyConfig;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;

pub struct RequestStatusHandler {}

impl RequestStatusHandler {
    pub fn path_prefix() -> &'static str {
        "/api/1/requestStatus/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        let (head, body) = req.into_parts();
        if head.method != Method::GET {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?);
        }
        let accept = head
            .headers
            .get(http::header::ACCEPT)
            .map_or(Ok(ACCEPT_ALL), |k| k.to_str())
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?
            .to_owned();
        if accept != APP_JSON && accept != ACCEPT_ALL {
            // TODO set the public error code and message and return Err(e).
            let e = Error::with_public_msg_no_trace(format!("Unsupported Accept: {:?}", accept));
            error!("{e}");
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let _body_data = read_body_bytes(body).await?;
        let status_id = &head.uri.path()[Self::path_prefix().len()..];
        debug!("RequestStatusHandler  status_id {:?}", status_id);

        let back = {
            let mut ret = None;
            for b in &proxy_config.backends {
                if b.name == "sf-databuffer" {
                    ret = Some(b);
                    break;
                }
            }
            ret
        };
        if let Some(back) = back {
            let url_str = format!("{}{}{}", back.url, Self::path_prefix(), status_id);
            debug!("try to ask {url_str}");
            let uri: Uri = url_str.parse()?;
            let req = Request::builder()
                .method(Method::GET)
                .header(header::HOST, uri.host().unwrap())
                .uri(&uri)
                .body(body_empty())?;
            let res = connect_client(&uri).await?.send_request(req).await?;
            let (head, body) = res.into_parts();
            if head.status != StatusCode::OK {
                error!("backend returned error: {head:?}");
                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
            } else {
                debug!("backend returned OK");
                let body = read_body_bytes(body).await?;
                Ok(response(StatusCode::OK).body(body_bytes(body))?)
            }
        } else {
            Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
        }
    }
}
