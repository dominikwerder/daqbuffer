use crate::events2::prepare::StmtsCache;
use crate::worker::ScyllaQueue;
use futures_util::StreamExt;
use items_0::timebin::BinsBoxed;
use items_2::binning::container_bins::ContainerBins;
use netpod::DtMs;
use netpod::TsNano;
use scylla::Session as ScySession;
use std::ops::Range;

pub struct ScyllaCacheReadProvider {
    scyqueue: ScyllaQueue,
}

impl ScyllaCacheReadProvider {
    pub fn new(scyqueue: ScyllaQueue) -> Self {
        Self { scyqueue }
    }
}

impl streams::timebin::CacheReadProvider for ScyllaCacheReadProvider {
    fn read(
        &self,
        series: u64,
        bin_len: DtMs,
        msp: u64,
        offs: Range<u32>,
    ) -> streams::timebin::cached::reader::CacheReading {
        let scyqueue = self.scyqueue.clone();
        // let fut = async move { scyqueue.read_cache_f32(series, bin_len, msp, offs).await };
        let fut = async { todo!("TODO impl scylla cache read") };
        streams::timebin::cached::reader::CacheReading::new(Box::pin(fut))
    }

    fn write(&self, series: u64, bins: BinsBoxed) -> streams::timebin::cached::reader::CacheWriting {
        let scyqueue = self.scyqueue.clone();
        let bins = todo!("TODO impl scylla cache write");
        let fut = async move { scyqueue.write_cache_f32(series, bins).await };
        streams::timebin::cached::reader::CacheWriting::new(Box::pin(fut))
    }
}

pub async fn worker_write(
    series: u64,
    bins: ContainerBins<f32>,
    stmts_cache: &StmtsCache,
    scy: &ScySession,
) -> Result<(), streams::timebin::cached::reader::Error> {
    for ((((((&ts1, &ts2), &cnt), &min), &max), &avg), &lst) in bins.zip_iter() {
        let bin_len = DtMs::from_ms_u64((ts2.ns() - ts1.ns()) / 1000000);
        let div = streams::timebin::cached::reader::part_len(bin_len).ns();
        let msp = ts1.ns() / div;
        let off = (ts1.ns() - msp * div) / bin_len.ns();
        let params = (
            series as i64,
            bin_len.ms() as i32,
            msp as i64,
            off as i32,
            cnt as i64,
            min,
            max,
            avg,
            lst,
        );
        // trace!("cache write {:?}", params);
        scy.execute(stmts_cache.st_write_f32(), params)
            .await
            .map_err(|e| streams::timebin::cached::reader::Error::Scylla(e.to_string()))?;
    }
    Ok(())
}

pub async fn worker_read(
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: core::ops::Range<u32>,
    stmts_cache: &StmtsCache,
    scy: &ScySession,
) -> Result<ContainerBins<f32>, streams::timebin::cached::reader::Error> {
    let div = streams::timebin::cached::reader::part_len(bin_len).ns();
    let params = (
        series as i64,
        bin_len.ms() as i32,
        msp as i64,
        offs.start as i32,
        offs.end as i32,
    );
    let res = scy
        .execute_iter(stmts_cache.st_read_f32().clone(), params)
        .await
        .map_err(|e| streams::timebin::cached::reader::Error::Scylla(e.to_string()))?;
    let mut it = res.into_typed::<(i32, i64, f32, f32, f32, f32)>();
    let mut bins = ContainerBins::new();
    while let Some(x) = it.next().await {
        let row = x.map_err(|e| streams::timebin::cached::reader::Error::Scylla(e.to_string()))?;
        let off = row.0 as u64;
        let cnt = row.1 as u64;
        let min = row.2;
        let max = row.3;
        let avg = row.4;
        let lst = row.5;
        let ts1 = TsNano::from_ns(bin_len.ns() * off + div * msp);
        let ts2 = TsNano::from_ns(ts1.ns() + bin_len.ns());
        // By assumption, bins which got written to storage are considered final
        let fnl = true;
        bins.push_back(ts1, ts2, cnt, min, max, avg, lst, fnl);
    }
    Ok(bins)
}
