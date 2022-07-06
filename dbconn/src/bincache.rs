use crate::events_scylla::EventsStreamScylla;
use crate::ErrConv;
use err::Error;
use futures_util::{Future, Stream, StreamExt};
use items::binsdim0::MinMaxAvgDim0Bins;
use items::{empty_binned_dyn, empty_events_dyn, RangeCompletableItem, StreamItem, TimeBinned};
use netpod::log::*;
use netpod::query::{CacheUsage, RawEventsQuery};
use netpod::timeunits::*;
use netpod::{
    AggKind, ChannelTyped, PreBinnedPatchCoord, PreBinnedPatchIterator, PreBinnedPatchRange, ScalarType, ScyllaConfig,
    Shape,
};
use scylla::Session as ScySession;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub async fn read_cached_scylla(
    series: u64,
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    _agg_kind: AggKind,
    scy: &ScySession,
) -> Result<Option<Box<dyn TimeBinned>>, Error> {
    let vals = (
        series as i64,
        (coord.bin_t_len() / SEC) as i32,
        (coord.patch_t_len() / SEC) as i32,
        coord.ix() as i64,
    );
    let res = scy
            .query_iter(
                "select counts, avgs, mins, maxs from binned_scalar_f32 where series = ? and bin_len_sec = ? and patch_len_sec = ? and agg_kind = 'dummy-agg-kind' and offset = ?",
                vals,
            )
            .await;
    let mut res = res.err_conv().map_err(|e| {
        error!("can not read from cache");
        e
    })?;
    while let Some(item) = res.next().await {
        let row = item.err_conv()?;
        let edges = coord.edges();
        let (counts, avgs, mins, maxs): (Vec<i64>, Vec<f32>, Vec<f32>, Vec<f32>) = row.into_typed().err_conv()?;
        let mut counts_mismatch = false;
        if edges.len() != counts.len() + 1 {
            counts_mismatch = true;
        }
        if counts.len() != avgs.len() {
            counts_mismatch = true;
        }
        let ts1s = edges[..(edges.len() - 1).min(edges.len())].to_vec();
        let ts2s = edges[1.min(edges.len())..].to_vec();
        if ts1s.len() != ts2s.len() {
            error!("ts1s vs ts2s mismatch");
            counts_mismatch = true;
        }
        if ts1s.len() != counts.len() {
            counts_mismatch = true;
        }
        let avgs = avgs.into_iter().map(|x| x).collect::<Vec<_>>();
        let mins = mins.into_iter().map(|x| x as _).collect::<Vec<_>>();
        let maxs = maxs.into_iter().map(|x| x as _).collect::<Vec<_>>();
        if counts_mismatch {
            error!(
                "mismatch:  edges {}  ts1s {}  ts2s {}  counts {}  avgs {}  mins {}  maxs {}",
                edges.len(),
                ts1s.len(),
                ts2s.len(),
                counts.len(),
                avgs.len(),
                mins.len(),
                maxs.len(),
            );
        }
        let counts: Vec<_> = counts.into_iter().map(|x| x as u64).collect();
        // TODO construct a dyn TimeBinned using the scalar type and shape information.
        // TODO place the values with little copying into the TimeBinned.
        use ScalarType::*;
        use Shape::*;
        match &chn.shape {
            Scalar => match &chn.scalar_type {
                F64 => {
                    let ret = MinMaxAvgDim0Bins::<f64> {
                        ts1s,
                        ts2s,
                        counts,
                        avgs,
                        mins,
                        maxs,
                    };
                    return Ok(Some(Box::new(ret)));
                }
                _ => {
                    error!("TODO can not yet restore {:?} {:?}", chn.scalar_type, chn.shape);
                    err::todoval()
                }
            },
            _ => {
                error!("TODO can not yet restore {:?} {:?}", chn.scalar_type, chn.shape);
                err::todoval()
            }
        }
    }
    Ok(None)
}

#[allow(unused)]
struct WriteFut<'a> {
    chn: &'a ChannelTyped,
    coord: &'a PreBinnedPatchCoord,
    data: &'a dyn TimeBinned,
    scy: &'a ScySession,
}

impl<'a> WriteFut<'a> {
    fn new(
        chn: &'a ChannelTyped,
        coord: &'a PreBinnedPatchCoord,
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
    chn: &ChannelTyped,
    coord: &'a PreBinnedPatchCoord,
    data: &'a dyn TimeBinned,
    scy: &ScySession,
) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
    let _chn = unsafe { &*(chn as *const ChannelTyped) };
    let data = unsafe { &*(data as *const dyn TimeBinned) };
    let scy = unsafe { &*(scy as *const ScySession) };
    let fut = async move {
        let bin_len_sec = (coord.bin_t_len() / SEC) as i32;
        let patch_len_sec = (coord.patch_t_len() / SEC) as i32;
        let offset = coord.ix();
        warn!(
            "write_cached_scylla  len {}  where series = {} and bin_len_sec = {} and patch_len_sec = {} and agg_kind = 'dummy-agg-kind' and offset = {}",
            series,
            bin_len_sec,
            patch_len_sec,
            offset,
            data.counts().len()
        );
        let stmt = scy.prepare("insert into binned_scalar_f32 (series, bin_len_sec, patch_len_sec, agg_kind, offset, counts, avgs, mins, maxs) values (?, ?, ?, 'dummy-agg-kind', ?, ?, ?, ?, ?)").await.err_conv()?;
        scy.execute(
            &stmt,
            (
                series as i64,
                bin_len_sec,
                patch_len_sec,
                offset as i64,
                data.counts().iter().map(|x| *x as i64).collect::<Vec<_>>(),
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
        Ok(())
    };
    Box::pin(fut)
}

pub async fn fetch_uncached_data(
    series: u64,
    chn: ChannelTyped,
    coord: PreBinnedPatchCoord,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Result<Option<(Box<dyn TimeBinned>, bool)>, Error> {
    info!("fetch_uncached_data  {coord:?}");
    // Try to find a higher resolution pre-binned grid which covers the requested patch.
    let (bin, complete) = match PreBinnedPatchRange::covering_range(coord.patch_range(), coord.bin_count() + 1) {
        Ok(Some(range)) => {
            fetch_uncached_higher_res_prebinned(series, &chn, range, agg_kind, cache_usage.clone(), scy.clone()).await
        }
        Ok(None) => fetch_uncached_binned_events(series, &chn, coord.clone(), agg_kind, scy.clone()).await,
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
            WriteFut::new(&chn, &coord, bin.as_ref(), &scy).await?;
            write_cached_scylla(series, &chn, &coord, bin.as_ref(), &scy).await?;
        }
    }
    Ok(Some((bin, complete)))
}

pub fn fetch_uncached_data_box(
    series: u64,
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Pin<Box<dyn Future<Output = Result<Option<(Box<dyn TimeBinned>, bool)>, Error>> + Send>> {
    Box::pin(fetch_uncached_data(
        series,
        chn.clone(),
        coord.clone(),
        agg_kind,
        cache_usage,
        scy,
    ))
}

pub async fn fetch_uncached_higher_res_prebinned(
    series: u64,
    chn: &ChannelTyped,
    range: PreBinnedPatchRange,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Result<(Box<dyn TimeBinned>, bool), Error> {
    // TODO refine the AggKind scheme or introduce a new BinningOpts type and get time-weight from there.
    let do_time_weight = true;
    // We must produce some result with correct types even if upstream delivers nothing at all.
    let bin0 = empty_binned_dyn(&chn.scalar_type, &chn.shape, &agg_kind);
    let mut time_binner = bin0.time_binner_new(range.edges(), do_time_weight);
    let mut complete = true;
    let patch_it = PreBinnedPatchIterator::from_range(range.clone());
    for patch in patch_it {
        // We request data here for a Coord, meaning that we expect to receive multiple bins.
        // The expectation is that we receive a single TimeBinned which contains all bins of that PatchCoord.
        let coord = PreBinnedPatchCoord::new(patch.bin_t_len(), patch.patch_t_len(), patch.ix());
        let (bin, comp) =
            pre_binned_value_stream_with_scy(series, chn, &coord, agg_kind.clone(), cache_usage.clone(), scy.clone())
                .await?;
        complete = complete & comp;
        time_binner.ingest(bin.as_time_binnable_dyn());
    }
    // Fixed limit to defend against a malformed implementation:
    let mut i = 0;
    while i < 80000 && time_binner.bins_ready_count() < range.bin_count() as usize {
        if false {
            trace!(
                "extra cycle {}  {}  {}",
                i,
                time_binner.bins_ready_count(),
                range.bin_count()
            );
        }
        time_binner.cycle();
        i += 1;
    }
    if time_binner.bins_ready_count() < range.bin_count() as usize {
        return Err(Error::with_msg_no_trace(format!(
            "unable to produce all bins for the patch range {} vs {}",
            time_binner.bins_ready_count(),
            range.bin_count(),
        )));
    }
    let ready = time_binner
        .bins_ready()
        .ok_or_else(|| Error::with_msg_no_trace(format!("unable to produce any bins for the patch range")))?;
    Ok((ready, complete))
}

pub async fn fetch_uncached_binned_events(
    series: u64,
    chn: &ChannelTyped,
    coord: PreBinnedPatchCoord,
    agg_kind: AggKind,
    scy: Arc<ScySession>,
) -> Result<(Box<dyn TimeBinned>, bool), Error> {
    // TODO refine the AggKind scheme or introduce a new BinningOpts type and get time-weight from there.
    let do_time_weight = true;
    // We must produce some result with correct types even if upstream delivers nothing at all.
    let bin0 = empty_events_dyn(&chn.scalar_type, &chn.shape, &agg_kind);
    let mut time_binner = bin0.time_binner_new(coord.edges(), do_time_weight);
    let deadline = Instant::now();
    let deadline = deadline
        .checked_add(Duration::from_millis(6000))
        .ok_or_else(|| Error::with_msg_no_trace(format!("deadline overflow")))?;
    let evq = RawEventsQuery::new(chn.channel.clone(), coord.patch_range(), AggKind::Plain);
    let mut events_dyn = EventsStreamScylla::new(series, &evq, chn.scalar_type.clone(), chn.shape.clone(), scy, false);
    let mut complete = false;
    loop {
        let item = tokio::time::timeout_at(deadline.into(), events_dyn.next()).await;
        let item = match item {
            Ok(Some(k)) => k,
            Ok(None) => break,
            Err(_) => {
                error!("fetch_uncached_binned_events  timeout");
                return Err(Error::with_msg_no_trace(format!(
                    "TODO handle fetch_uncached_binned_events timeout"
                )));
            }
        };
        match item {
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => {
                time_binner.ingest(item.as_time_binnable_dyn());
                // TODO could also ask the binner here whether we are "complete" to stop sending useless data.
            }
            Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => {
                complete = true;
            }
            Ok(StreamItem::Stats(_item)) => {
                warn!("TODO forward in stream bincache stats");
            }
            Ok(StreamItem::Log(item)) => {
                warn!("TODO forward in stream bincache log msg {}", item.msg);
            }
            Err(e) => return Err(e),
        }
    }
    // Fixed limit to defend against a malformed implementation:
    let mut i = 0;
    while i < 80000 && time_binner.bins_ready_count() < coord.bin_count() as usize {
        if false {
            trace!(
                "extra cycle {}  {}  {}",
                i,
                time_binner.bins_ready_count(),
                coord.bin_count()
            );
        }
        time_binner.cycle();
        i += 1;
    }
    if time_binner.bins_ready_count() < coord.bin_count() as usize {
        return Err(Error::with_msg_no_trace(format!(
            "unable to produce all bins for the patch range {} vs {}",
            time_binner.bins_ready_count(),
            coord.bin_count(),
        )));
    }
    let ready = time_binner
        .bins_ready()
        .ok_or_else(|| Error::with_msg_no_trace(format!("unable to produce any bins for the patch")))?;
    Ok((ready, complete))
}

pub async fn pre_binned_value_stream_with_scy(
    series: u64,
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Result<(Box<dyn TimeBinned>, bool), Error> {
    trace!("pre_binned_value_stream_with_scy  {chn:?}  {coord:?}");
    if let (Some(item), CacheUsage::Use) = (
        read_cached_scylla(series, chn, coord, agg_kind.clone(), &scy).await?,
        &cache_usage,
    ) {
        info!("+++++++++++++    GOOD READ");
        Ok((item, true))
    } else {
        if let CacheUsage::Use = &cache_usage {
            warn!("--+--+--+--+--+--+   NOT YET CACHED");
        }
        let res = fetch_uncached_data_box(series, chn, coord, agg_kind, cache_usage, scy).await?;
        let (bin, complete) =
            res.ok_or_else(|| Error::with_msg_no_trace(format!("pre_binned_value_stream_with_scy got None bin")))?;
        Ok((bin, complete))
    }
}

pub async fn pre_binned_value_stream(
    series: u64,
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    scyconf: &ScyllaConfig,
) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn TimeBinned>, Error>> + Send>>, Error> {
    trace!("pre_binned_value_stream  series {series}  {chn:?}  {coord:?}  {scyconf:?}");
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .use_keyspace(&scyconf.keyspace, true)
        .build()
        .await
        .err_conv()?;
    let scy = Arc::new(scy);
    let res = pre_binned_value_stream_with_scy(series, chn, coord, agg_kind, cache_usage, scy).await?;
    Ok(Box::pin(futures_util::stream::iter([Ok(res.0)])))
}
