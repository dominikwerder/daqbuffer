#![allow(unused)]

use crate::errconv::ErrConv;
use crate::events2::prepare::StmtsCache;
use crate::worker::ScyllaQueue;
use err::Error;
use futures_util::Future;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::timebin::TimeBinned;
use items_0::Empty;
use items_2::binsdim0::BinsDim0;
use items_2::channelevents::ChannelEvents;
use netpod::log::*;
use netpod::query::CacheUsage;
use netpod::timeunits::*;
use netpod::AggKind;
use netpod::BinnedRange;
use netpod::ChannelTyped;
use netpod::Dim0Kind;
use netpod::DtMs;
use netpod::PreBinnedPatchCoord;
use netpod::PreBinnedPatchCoordEnum;
use netpod::PreBinnedPatchRange;
use netpod::PreBinnedPatchRangeEnum;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use query::transform::TransformQuery;
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

#[allow(unused)]
struct WriteFut<'a> {
    chn: &'a ChannelTyped,
    coord: &'a PreBinnedPatchCoordEnum,
    data: &'a dyn TimeBinned,
    scy: &'a ScySession,
}

impl<'a> WriteFut<'a> {
    #[allow(unused)]
    fn new(
        chn: &'a ChannelTyped,
        coord: &'a PreBinnedPatchCoordEnum,
        data: &'a dyn TimeBinned,
        scy: &'a ScySession,
    ) -> Self {
        Self { chn, coord, data, scy }
    }
}

impl<'a> Future for WriteFut<'a> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let _ = cx;
        Poll::Ready(Ok(()))
    }
}

pub fn write_cached_scylla<'a>(
    series: u64,
    _chn: &'a ChannelTyped,
    coord: &'a PreBinnedPatchCoordEnum,
    //data: &'a dyn TimeBinned,
    data: Box<dyn TimeBinned>,
    //scy: &'a ScySession,
    _scy: Arc<ScySession>,
) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
    //let _chn = unsafe { &*(chn as *const ChannelTyped) };
    //let data_ptr = data as *const dyn TimeBinned as usize;
    //let scy_ptr = scy as *const ScySession as usize;
    let fut = async move {
        //let data = unsafe { &*(data_ptr as *const dyn TimeBinned) };
        //let scy = unsafe { &*(scy_ptr as *const ScySession) };
        todo!();
        /*let bin_len_sec = (coord.bin_t_len() / SEC) as i32;
        let patch_len_sec = (coord.patch_t_len() / SEC) as i32;
        let offset = coord.ix();*/
        let bin_len_sec = 0i32;
        let patch_len_sec = 0i32;
        let offset = 0i32;
        warn!(
            "write_cached_scylla  len {}  where series = {} and bin_len_sec = {} and patch_len_sec = {} and agg_kind = 'dummy-agg-kind' and offset = {}",
            data.counts().len(),
            series,
            bin_len_sec,
            patch_len_sec,
            offset,
        );
        let _data2 = data.counts().iter().map(|x| *x as i64).collect::<Vec<_>>();
        /*
        let stmt = scy.prepare("insert into binned_scalar_f32 (series, bin_len_sec, patch_len_sec, agg_kind, offset, counts, avgs, mins, maxs) values (?, ?, ?, 'dummy-agg-kind', ?, ?, ?, ?, ?)").await.err_conv()?;
        scy.execute(
            &stmt,
            (
                series as i64,
                bin_len_sec,
                patch_len_sec,
                offset as i64,
                data2,
                data.avgs(),
                data.mins(),
                data.maxs(),
            ),
        )
        .await
        .err_conv()
        .map_err(|e| {
            error!("can not write to cache");
            e
        })?;
        */
        Ok(())
    };
    Box::pin(fut)
}

pub async fn fetch_uncached_data(
    series: u64,
    chn: ChannelTyped,
    coord: PreBinnedPatchCoordEnum,
    one_before_range: bool,
    transform: TransformQuery,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Result<Option<(Box<dyn TimeBinned>, bool)>, Error> {
    /*info!("fetch_uncached_data  {coord:?}");
    // Try to find a higher resolution pre-binned grid which covers the requested patch.
    let (bin, complete) = match PreBinnedPatchRange::covering_range(coord.patch_range(), coord.bin_count() + 1) {
        Ok(Some(range)) => {
            if coord.patch_range() != range.range() {
                error!(
                    "The chosen covering range does not exactly cover the requested patch  {:?}  vs  {:?}",
                    coord.patch_range(),
                    range.range()
                );
            }
            fetch_uncached_higher_res_prebinned(
                series,
                &chn,
                coord.clone(),
                range,
                one_before_range,
                transform,
                cache_usage.clone(),
                scy.clone(),
            )
            .await
        }
        Ok(None) => {
            fetch_uncached_binned_events(series, &chn, coord.clone(), one_before_range, transform, scy.clone()).await
        }
        Err(e) => Err(e),
    }?;
    if true || complete {
        let edges = coord.edges();
        if edges.len() < bin.len() + 1 {
            error!(
                "attempt to write overfull bin to cache  edges {}  bin {}",
                edges.len(),
                bin.len()
            );
            return Err(Error::with_msg_no_trace(format!(
                "attempt to write overfull bin to cache"
            )));
        } else if edges.len() > bin.len() + 1 {
            let missing = edges.len() - bin.len() - 1;
            error!("attempt to write incomplete bin to cache missing {missing}");
        }
        if let CacheUsage::Use | CacheUsage::Recreate = &cache_usage {
            // TODO pass data in safe way.
            let _data = bin.as_ref();
            //let fut = WriteFut::new(&chn, &coord, err::todoval(), &scy);
            //fut.await?;
            //write_cached_scylla(series, &chn, &coord, bin.as_ref(), &scy).await?;
        }
    }
    Ok(Some((bin, complete)))*/
    todo!()
}

pub fn fetch_uncached_data_box(
    series: u64,
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoordEnum,
    one_before_range: bool,
    transform: TransformQuery,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Pin<Box<dyn Future<Output = Result<Option<(Box<dyn TimeBinned>, bool)>, Error>> + Send>> {
    Box::pin(fetch_uncached_data(
        series,
        chn.clone(),
        coord.clone(),
        one_before_range,
        transform,
        cache_usage,
        scy,
    ))
}

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
        let fut = async move { scyqueue.read_cache_f32(series, bin_len, msp, offs).await };
        streams::timebin::cached::reader::CacheReading::new(Box::pin(fut))
    }

    fn write(&self, series: u64, bins: BinsDim0<f32>) -> streams::timebin::cached::reader::CacheWriting {
        let scyqueue = self.scyqueue.clone();
        let fut = async move { scyqueue.write_cache_f32(series, bins).await };
        streams::timebin::cached::reader::CacheWriting::new(Box::pin(fut))
    }
}

pub async fn worker_write(
    series: u64,
    bins: BinsDim0<f32>,
    stmts_cache: &StmtsCache,
    scy: &ScySession,
) -> Result<(), streams::timebin::cached::reader::Error> {
    let mut msp_last = u64::MAX;
    for (((((&ts1, &ts2), &cnt), &min), &max), &avg) in bins
        .ts1s
        .iter()
        .zip(bins.ts2s.iter())
        .zip(bins.counts.iter())
        .zip(bins.mins.iter())
        .zip(bins.maxs.iter())
        .zip(bins.avgs.iter())
    {
        let bin_len = DtMs::from_ms_u64((ts2 - ts1) / 1000000);
        let div = streams::timebin::cached::reader::part_len(bin_len).ns();
        let msp = ts1 / div;
        let off = (ts1 - msp * div) / bin_len.ns();
        let params = (
            series as i64,
            bin_len.ms() as i32,
            msp as i64,
            off as i32,
            cnt as i64,
            min,
            max,
            avg,
        );
        eprintln!("cache write {:?}", params);
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
) -> Result<BinsDim0<f32>, streams::timebin::cached::reader::Error> {
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
    let mut it = res.into_typed::<(i32, i64, f32, f32, f32)>();
    let mut bins = BinsDim0::empty();
    while let Some(x) = it.next().await {
        let row = x.map_err(|e| streams::timebin::cached::reader::Error::Scylla(e.to_string()))?;
        let off = row.0 as u64;
        let cnt = row.1 as u64;
        let min = row.2;
        let max = row.3;
        let avg = row.4;
        let ts1 = bin_len.ns() * off + div * msp;
        let ts2 = ts1 + bin_len.ns();
        bins.push(ts1, ts2, cnt, min, max, avg);
    }
    Ok(bins)
}
