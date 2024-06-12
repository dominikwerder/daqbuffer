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
use netpod::Shape;
use query::api4::AccountingIngestedBytesQuery;
use query::api4::AccountingToplistQuery;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

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
    dim0: Vec<(String, u64, u64)>,
    dim1: Vec<(String, u64, u64)>,
    infos_count_total: usize,
    infos_missing_count: usize,
    top1_usage_len: usize,
    scalar_count: usize,
    wave_count: usize,
    found: usize,
    incomplete_count: usize,
    mismatch_count: usize,
}

impl Toplist {
    fn new() -> Self {
        Self {
            dim0: Vec::new(),
            dim1: Vec::new(),
            infos_count_total: 0,
            infos_missing_count: 0,
            top1_usage_len: 0,
            scalar_count: 0,
            wave_count: 0,
            found: 0,
            incomplete_count: 0,
            mismatch_count: 0,
        }
    }
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
        let list_len_max = qu.limit() as usize;
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
        top1.sort_by_counts();
        let mut ret = Toplist::new();
        let top1_usage = top1.usage();
        ret.top1_usage_len = top1_usage.len();
        let usage_map_0: BTreeMap<u64, (u64, u64)> = top1_usage.iter().map(|x| (x.0, (x.1, x.2))).collect();
        let mut usage_it = usage_map_0.iter();
        loop {
            let mut series_ids = Vec::new();
            let mut usages = Vec::new();
            while let Some(u) = usage_it.next() {
                series_ids.push(*u.0);
                usages.push(u.1.clone());
                if series_ids.len() >= 200 {
                    break;
                }
            }
            if series_ids.len() == 0 {
                break;
            }
            let infos = dbconn::channelinfo::info_for_series_ids(&series_ids, &pg)
                .await
                .map_err(Error::from_to_string)?;
            for (_series, info_res) in &infos {
                if let Some(info) = info_res {
                    match &info.shape {
                        Shape::Scalar => {
                            ret.scalar_count += 1;
                        }
                        Shape::Wave(_) => {
                            ret.wave_count += 1;
                        }
                        _ => {}
                    }
                }
            }
            if usages.len() > infos.len() {
                ret.incomplete_count += usages.len() - infos.len();
            }
            if infos.len() > usages.len() {
                ret.incomplete_count += infos.len() - usages.len();
            }
            for ((series2, info_res), usage) in infos.into_iter().zip(usages.into_iter()) {
                if let Some(info) = info_res {
                    if series2 != info.series {
                        ret.mismatch_count += 1;
                    }
                    ret.infos_count_total += 1;
                    // if info.name == "SINSB04-RMOD:PULSE-I-WF" {
                    //     ret.found += 1;
                    // }
                    match &info.shape {
                        Shape::Scalar => {
                            ret.dim0.push((info.name, usage.0, usage.1));
                        }
                        Shape::Wave(_) => {
                            ret.dim1.push((info.name, usage.0, usage.1));
                        }
                        Shape::Image(_, _) => {}
                    }
                } else {
                    ret.infos_missing_count += 1;
                }
            }
        }
        ret.dim0.sort_by_cached_key(|x| u64::MAX - x.1);
        ret.dim1.sort_by_cached_key(|x| u64::MAX - x.1);
        ret.dim0.truncate(list_len_max);
        ret.dim1.truncate(list_len_max);
        Ok(ret)
    }
}
