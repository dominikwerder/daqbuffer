pub mod reqstatus;

use crate::bodystream::response;
use crate::ReqCtx;
use err::Error;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use hyper::Client;
use netpod::log::*;
use netpod::query::api1::Api1Query;
use netpod::ProxyConfig;
use netpod::ACCEPT_ALL;
use netpod::X_DAQBUF_REQID;

pub struct PythonDataApi1Query {}

impl PythonDataApi1Query {
    pub fn path() -> &'static str {
        "/api/1/query"
    }

    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == Self::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Request<Body>,
        _ctx: &ReqCtx,
        proxy_config: &ProxyConfig,
    ) -> Result<Response<Body>, Error> {
        if req.method() != Method::POST {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?);
        }
        let (head, body) = req.into_parts();
        let _accept = head
            .headers
            .get(http::header::ACCEPT)
            .map_or(Ok(ACCEPT_ALL), |k| k.to_str())
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?
            .to_owned();
        let body_data = hyper::body::to_bytes(body).await?;
        if body_data.len() < 512 && body_data.first() == Some(&"{".as_bytes()[0]) {
            info!("request body_data string: {}", String::from_utf8_lossy(&body_data));
        }
        let qu = match serde_json::from_slice::<Api1Query>(&body_data) {
            Ok(qu) => qu,
            Err(e) => {
                error!("got body_data: {:?}", String::from_utf8_lossy(&body_data[..]));
                error!("can not parse: {e}");
                return Err(Error::with_msg_no_trace("can not parse query"));
            }
        };
        info!("Proxy sees request: {qu:?}");
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
            let url_str = format!("{}/api/1/query", back.url);
            info!("try to ask {url_str}");
            let req = Request::builder()
                .method(Method::POST)
                .uri(url_str)
                .body(Body::from(body_data))?;
            let client = Client::new();
            let res = client.request(req).await?;
            let (head, body) = res.into_parts();
            if head.status != StatusCode::OK {
                error!("backend returned error: {head:?}");
                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?)
            } else {
                info!("backend returned OK");
                let riq_def = HeaderValue::from_static("(none)");
                let riq = head.headers.get(X_DAQBUF_REQID).unwrap_or(&riq_def);
                Ok(response(StatusCode::OK).header(X_DAQBUF_REQID, riq).body(body)?)
            }
        } else {
            Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::empty())?)
        }
    }
}
