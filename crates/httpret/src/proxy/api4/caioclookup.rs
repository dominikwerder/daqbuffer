use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::ProxyConfig;

pub struct CaIocLookup {}

impl CaIocLookup {
    fn path() -> &'static str {
        "/api/4/channel-access/search/addr"
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
        node_config: &ProxyConfig,
    ) -> Result<Response<Body>, Error> {
        match self.search(req, ctx, node_config).await {
            Ok(status) => {
                let body = serde_json::to_vec(&status)?;
                let ret = response(StatusCode::OK).body(Body::from(body))?;
                Ok(ret)
            }
            Err(e) => {
                error!("sees: {e}");
                let ret = crate::bodystream::ToPublicResponse::to_public_response(&e);
                Ok(ret)
            }
        }
    }

    async fn search(
        &self,
        _req: Request<Body>,
        _ctx: &ReqCtx,
        _proxy_config: &ProxyConfig,
    ) -> Result<Option<String>, Error> {
        Ok(None)
    }
}
