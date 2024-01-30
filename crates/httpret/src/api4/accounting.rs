use crate::bodystream::response;
use crate::err::Error;
use crate::requests::accepts_json_or_all;
use crate::ReqCtx;
use futures_util::StreamExt;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use items_0::Empty;
use items_0::Extendable;
use items_2::accounting::AccountingEvents;
use items_2::channelevents::ChannelStatusEvents;
use netpod::log::*;
use netpod::query::ChannelStateEventsQuery;
use netpod::req_uri_to_url;
use netpod::FromUrl;
use netpod::NodeConfigCached;

pub struct AccountingIngestedBytes {}

impl AccountingIngestedBytes {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with("/api/4/status/accounting/ingested/bytes/") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                let url = req_uri_to_url(req.uri())?;
                let q = ChannelStateEventsQuery::from_url(&url)?;
                match self.fetch_data(&q, node_config).await {
                    Ok(k) => {
                        let body = ToJsonBody::from(&k).into_body();
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => {
                        error!("{e}");
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(body_string(format!("{:?}", e.public_msg())))?)
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn fetch_data(
        &self,
        q: &ChannelStateEventsQuery,
        node_config: &NodeConfigCached,
    ) -> Result<AccountingEvents, Error> {
        let scyco = node_config
            .node_config
            .cluster
            .scylla
            .as_ref()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("no scylla configured")))?;
        let scy = scyllaconn::conn::create_scy_session(scyco).await?;
        // TODO so far, we sum over everything
        let series_id = 0;
        let mut stream = scyllaconn::accounting::AccountingStreamScylla::new(series_id, q.range().clone(), scy);
        let mut ret = AccountingEvents::empty();
        while let Some(item) = stream.next().await {
            let mut item = item?;
            ret.extend_from(&mut item);
        }
        Ok(ret)
    }
}
