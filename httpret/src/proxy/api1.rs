use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::query::api1::Api1Query;
use netpod::ProxyConfig;
use netpod::ACCEPT_ALL;

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
        ctx: &ReqCtx,
        proxy_config: &ProxyConfig,
    ) -> Result<Response<Body>, Error> {
        if req.method() != Method::POST {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?);
        }
        let (head, body) = req.into_parts();
        let accept = head
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
        Ok(response(StatusCode::OK).body(Body::empty())?)
    }
}
