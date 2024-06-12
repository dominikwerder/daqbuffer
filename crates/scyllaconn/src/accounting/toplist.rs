use crate::errconv::ErrConv;
use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits;
use netpod::EMIT_ACCOUNTING_SNAP;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session as ScySession;
use std::sync::Arc;

#[derive(Debug)]
pub struct UsageData {
    ts: u64,
    // (series, count, bytes)
    usage: Vec<(u64, u64, u64)>,
}

impl UsageData {
    pub fn new(ts: u64) -> Self {
        Self { ts, usage: Vec::new() }
    }

    pub fn ts(&self) -> u64 {
        self.ts
    }

    pub fn usage(&self) -> &[(u64, u64, u64)] {
        &self.usage
    }

    pub fn sort_by_counts(&mut self) {
        self.usage.sort_unstable_by(|a, b| b.1.cmp(&a.1))
    }

    pub fn sort_by_bytes(&mut self) {
        self.usage.sort_unstable_by(|a, b| b.2.cmp(&a.2))
    }
}

pub async fn read_ts(ts: u64, scy: Arc<ScySession>) -> Result<UsageData, Error> {
    // TODO  toplist::read_ts  refactor
    info!("TODO  toplist::read_ts  refactor");
    let snap = EMIT_ACCOUNTING_SNAP.ms() / 1000;
    info!("ts {ts}  snap {snap:?}");
    let ts = ts / timeunits::SEC / snap * snap;
    let ret = read_ts_inner(ts, scy).await?;
    Ok(ret)
}

async fn read_ts_inner(ts: u64, scy: Arc<ScySession>) -> Result<UsageData, Error> {
    type RowType = (i64, i64, i64);
    let cql = concat!("select series, count, bytes from lt_account_00 where part = ? and ts = ?");
    let qu = prep(cql, scy.clone()).await?;
    let mut ret = UsageData::new(ts);
    for part in 0..255_u32 {
        let mut res = scy
            .execute_iter(qu.clone(), (part as i32, ts as i64))
            .await
            .err_conv()?
            .into_typed::<RowType>();
        while let Some(row) = res.next().await {
            let row = row.map_err(Error::from_string)?;
            let series = row.0 as u64;
            let count = row.1 as u64;
            let bytes = row.2 as u64;
            ret.usage.push((series, count, bytes));
        }
    }
    Ok(ret)
}

async fn prep(cql: &str, scy: Arc<ScySession>) -> Result<PreparedStatement, Error> {
    scy.prepare(cql)
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("cql error {e}")))
}
