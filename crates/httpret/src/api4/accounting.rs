use crate::bodystream::response;
use crate::err::Error;
use crate::requests::accepts_json_or_all;
use crate::ReqCtx;
use err::ToPublicError;
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
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use query::api4::AccountingIngestedBytesQuery;
use query::api4::AccountingToplistQuery;
use serde::Deserialize;
use serde::Serialize;

pub struct AccountingIngestedBytes {}

impl AccountingIngestedBytes {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with("/api/4/accounting/ingested/bytes") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match self.handle_get(req, ctx, ncc).await {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        error!("{e}");
                        let e2 = e.to_public_error();
                        let s = serde_json::to_string(&e2)?;
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_string(s))?)
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn handle_get(&self, req: Requ, ctx: &ReqCtx, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        let url = req_uri_to_url(req.uri())?;
        let q = AccountingIngestedBytesQuery::from_url(&url)?;
        let res = self.fetch_data(q, ctx, ncc).await?;
        let body = ToJsonBody::from(&res).into_body();
        Ok(response(StatusCode::OK).body(body)?)
    }

    async fn fetch_data(
        &self,
        q: AccountingIngestedBytesQuery,
        _ctx: &ReqCtx,
        ncc: &NodeConfigCached,
    ) -> Result<AccountingEvents, Error> {
        let scyco = ncc
            .node_config
            .cluster
            .scylla_st()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("no scylla configured")))?;
        let scy = scyllaconn::conn::create_scy_session(scyco).await?;
        let mut stream = scyllaconn::accounting::totals::AccountingStreamScylla::new(q.range().try_into()?, scy);
        let mut ret = AccountingEvents::empty();
        while let Some(item) = stream.next().await {
            let mut item = item?;
            ret.extend_from(&mut item);
        }
        Ok(ret)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Toplist {
    toplist: Vec<(String, u64, u64)>,
}

pub struct AccountingToplistCounts {}

impl AccountingToplistCounts {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with("/api/4/accounting/toplist/counts") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match self.handle_get(req, ctx, ncc).await {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        error!("{e}");
                        let e2 = e.to_public_error();
                        let s = serde_json::to_string(&e2)?;
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_string(s))?)
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn handle_get(&self, req: Requ, ctx: &ReqCtx, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        let url = req_uri_to_url(req.uri())?;
        let qu = AccountingToplistQuery::from_url(&url)?;
        let res = self.fetch_data(qu, ctx, ncc).await?;
        let body = ToJsonBody::from(&res).into_body();
        Ok(response(StatusCode::OK).body(body)?)
    }

    async fn fetch_data(
        &self,
        qu: AccountingToplistQuery,
        _ctx: &ReqCtx,
        ncc: &NodeConfigCached,
    ) -> Result<Toplist, Error> {
        // TODO assumes that accounting data is in the LT keyspace
        let scyco = ncc
            .node_config
            .cluster
            .scylla_lt()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("no lt scylla configured")))?;
        let scy = scyllaconn::conn::create_scy_session(scyco).await?;
        let pgconf = &ncc.node_config.cluster.database;
        let (pg, pgjh) = dbconn::create_connection(&pgconf).await?;
        let mut top1 = scyllaconn::accounting::toplist::read_ts(qu.ts().ns(), scy).await?;
        top1.sort_by_bytes();
        let mut ret = Toplist { toplist: Vec::new() };
        let series_ids: Vec<_> = top1.usage().iter().take(qu.limit() as _).map(|x| x.0).collect();
        let infos = dbconn::channelinfo::info_for_series_ids(&series_ids, &pg)
            .await
            .map_err(Error::from_to_string)?;
        let mut it = top1.usage().iter();
        for info in infos {
            let h = it.next().ok_or_else(|| Error::with_msg_no_trace("logic error"))?;
            if info.series != h.0 {
                let e = Error::with_msg_no_trace(format!("mismatch {} != {}", info.series, h.0));
                warn!("{e}");
                return Err(e);
            }
            ret.toplist.push((info.name, h.1, h.2));
        }
        Ok(ret)
    }
}
