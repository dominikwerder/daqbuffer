use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use http::StatusCode;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::ProxyConfig;

pub struct CaIocLookup {}

impl CaIocLookup {
    fn path() -> &'static str {
        "/api/4/channel-access/search/addr"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == Self::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, node_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        match self.search(req, ctx, node_config).await {
            Ok(status) => {
                let ret = response(StatusCode::OK).body(ToJsonBody::from(&status).into_body())?;
                Ok(ret)
            }
            Err(e) => {
                error!("sees: {e}");
                let ret = crate::bodystream::ToPublicResponse::to_public_response(&e);
                Ok(ret)
            }
        }
    }

    async fn search(&self, _req: Requ, _ctx: &ReqCtx, _proxy_config: &ProxyConfig) -> Result<Option<String>, Error> {
        Ok(None)
    }
}
