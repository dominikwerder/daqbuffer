use crate::bodystream::response;
use crate::err::Error;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use hyper::Client;
use netpod::log::*;
use netpod::ProxyConfig;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;

pub struct RequestStatusHandler {}

impl RequestStatusHandler {
    pub fn path_prefix() -> &'static str {
        "/api/1/requestStatus/"
    }

    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>, proxy_config: &ProxyConfig) -> Result<Response<Body>, Error> {
        let (head, body) = req.into_parts();
        if head.method != Method::GET {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?);
        }
        let accept = head
            .headers
            .get(http::header::ACCEPT)
            .map_or(Ok(ACCEPT_ALL), |k| k.to_str())
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?
            .to_owned();
        if accept != APP_JSON && accept != ACCEPT_ALL {
            // TODO set the public error code and message and return Err(e).
            let e = Error::with_public_msg(format!("Unsupported Accept: {:?}", accept));
            error!("{e}");
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(Body::empty())?);
        }
        let _body_data = hyper::body::to_bytes(body).await?;
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
            let req = Request::builder()
                .method(Method::GET)
                .uri(url_str)
                .body(Body::empty())?;
            let client = Client::new();
            let res = client.request(req).await?;
            let (head, body) = res.into_parts();
            if head.status != StatusCode::OK {
                error!("backend returned error: {head:?}");
                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?)
            } else {
                debug!("backend returned OK");
                Ok(response(StatusCode::OK).body(body)?)
            }
        } else {
            Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?)
        }
    }
}
