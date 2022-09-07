use crate::errconv::ErrConv;
use crate::events::EventsStreamScylla;
use err::Error;
use futures_util::{Future, Stream, StreamExt};
use items_2::binsdim0::BinsDim0;
use items_2::{empty_binned_dyn, empty_events_dyn, ChannelEvents, TimeBinned};
use netpod::log::*;
use netpod::query::{CacheUsage, PlainEventsQuery, RawEventsQuery};
use netpod::timeunits::*;
use netpod::{AggKind, ChannelTyped, ScalarType, Shape};
use netpod::{PreBinnedPatchCoord, PreBinnedPatchIterator, PreBinnedPatchRange};
use scylla::Session as ScySession;
use std::collections::VecDeque;
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
        let ts1s: VecDeque<_> = edges[..(edges.len() - 1).min(edges.len())].iter().map(|&x| x).collect();
        let ts2s: VecDeque<_> = edges[1.min(edges.len())..].iter().map(|&x| x).collect();
        if ts1s.len() != ts2s.len() {
            error!("ts1s vs ts2s mismatch");
            counts_mismatch = true;
        }
        if ts1s.len() != counts.len() {
            counts_mismatch = true;
        }
        let avgs: VecDeque<_> = avgs.into_iter().map(|x| x).collect();
        let mins: VecDeque<_> = mins.into_iter().map(|x| x as _).collect();
        let maxs: VecDeque<_> = maxs.into_iter().map(|x| x as _).collect();
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
        let counts: VecDeque<_> = counts.into_iter().map(|x| x as u64).collect();
        // TODO construct a dyn TimeBinned using the scalar type and shape information.
        // TODO place the values with little copying into the TimeBinned.
        use ScalarType::*;
        use Shape::*;
        match &chn.shape {
            Scalar => match &chn.scalar_type {
                F64 => {
                    let ret = BinsDim0::<f64> {
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
    #[allow(unused)]
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
    _chn: &'a ChannelTyped,
    coord: &'a PreBinnedPatchCoord,
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
        let bin_len_sec = (coord.bin_t_len() / SEC) as i32;
        let patch_len_sec = (coord.patch_t_len() / SEC) as i32;
        let offset = coord.ix();
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
    coord: PreBinnedPatchCoord,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Result<Option<(Box<dyn TimeBinned>, bool)>, Error> {
    info!("fetch_uncached_data  {coord:?}");
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
                agg_kind,
                cache_usage.clone(),
                scy.clone(),
            )
            .await
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
            // TODO pass data in safe way.
            let _data = bin.as_ref();
            //let fut = WriteFut::new(&chn, &coord, err::todoval(), &scy);
            //fut.await?;
            //write_cached_scylla(series, &chn, &coord, bin.as_ref(), &scy).await?;
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
    coord: PreBinnedPatchCoord,
    range: PreBinnedPatchRange,
    agg_kind: AggKind,
    cache_usage: CacheUsage,
    scy: Arc<ScySession>,
) -> Result<(Box<dyn TimeBinned>, bool), Error> {
    let edges = coord.edges();
    // TODO refine the AggKind scheme or introduce a new BinningOpts type and get time-weight from there.
    let do_time_weight = true;
    // We must produce some result with correct types even if upstream delivers nothing at all.
    let bin0 = empty_binned_dyn(&chn.scalar_type, &chn.shape, &agg_kind);
    let mut time_binner = bin0.time_binner_new(edges.clone(), do_time_weight);
    let mut complete = true;
    let patch_it = PreBinnedPatchIterator::from_range(range.clone());
    for patch_coord in patch_it {
        // We request data here for a Coord, meaning that we expect to receive multiple bins.
        // The expectation is that we receive a single TimeBinned which contains all bins of that PatchCoord.
        //let patch_coord = PreBinnedPatchCoord::new(patch.bin_t_len(), patch.patch_t_len(), patch.ix());
        let (bin, comp) = pre_binned_value_stream_with_scy(
            series,
            chn,
            &patch_coord,
            agg_kind.clone(),
            cache_usage.clone(),
            scy.clone(),
        )
        .await?;
        if let Err(msg) = bin.validate() {
            error!(
                "pre-binned intermediate issue {}  coord {:?}  patch_coord {:?}",
                msg, coord, patch_coord
            );
        }
        complete = complete && comp;
        time_binner.ingest(bin.as_time_binnable_dyn());
    }
    // Fixed limit to defend against a malformed implementation:
    let mut i = 0;
    while i < 80000 && time_binner.bins_ready_count() < coord.bin_count() as usize {
        let n1 = time_binner.bins_ready_count();
        if false {
            trace!(
                "pre-binned extra cycle  {}  {}  {}",
                i,
                time_binner.bins_ready_count(),
                coord.bin_count()
            );
        }
        time_binner.cycle();
        i += 1;
        if time_binner.bins_ready_count() <= n1 {
            warn!("pre-binned cycle did not add another bin, break");
            break;
        }
    }
    if time_binner.bins_ready_count() < coord.bin_count() as usize {
        return Err(Error::with_msg_no_trace(format!(
            "pre-binned unable to produce all bins for the patch  bins_ready {}  coord.bin_count {}  edges.len {}",
            time_binner.bins_ready_count(),
            coord.bin_count(),
            edges.len(),
        )));
    }
    let ready = time_binner
        .bins_ready()
        .ok_or_else(|| Error::with_msg_no_trace(format!("unable to produce any bins for the patch range")))?;
    if let Err(msg) = ready.validate() {
        error!("pre-binned final issue {}  coord {:?}", msg, coord);
    }
    Ok((ready, complete))
}

pub async fn fetch_uncached_binned_events(
    series: u64,
    chn: &ChannelTyped,
    coord: PreBinnedPatchCoord,
    agg_kind: AggKind,
    scy: Arc<ScySession>,
) -> Result<(Box<dyn TimeBinned>, bool), Error> {
    let edges = coord.edges();
    // TODO refine the AggKind scheme or introduce a new BinningOpts type and get time-weight from there.
    let do_time_weight = true;
    // We must produce some result with correct types even if upstream delivers nothing at all.
    let bin0 = empty_events_dyn(&chn.scalar_type, &chn.shape, &agg_kind);
    let mut time_binner = bin0.time_binner_new(edges.clone(), do_time_weight);
    let deadline = Instant::now();
    let deadline = deadline
        .checked_add(Duration::from_millis(6000))
        .ok_or_else(|| Error::with_msg_no_trace(format!("deadline overflow")))?;
    let _evq = RawEventsQuery::new(chn.channel.clone(), coord.patch_range(), agg_kind);
    let evq = PlainEventsQuery::new(chn.channel.clone(), coord.patch_range(), 4096, None, true);
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
            Ok(ChannelEvents::Events(item)) => {
                time_binner.ingest(item.as_time_binnable());
                // TODO could also ask the binner here whether we are "complete" to stop sending useless data.
            }
            Ok(ChannelEvents::Status(_)) => {
                // TODO flag, should not happen.
                return Err(Error::with_msg_no_trace(format!(
                    "unexpected read of channel status events"
                )));
            }
            Ok(ChannelEvents::RangeComplete) => {
                complete = true;
            }
            Err(e) => return Err(e),
        }
    }
    // Fixed limit to defend against a malformed implementation:
    let mut i = 0;
    while i < 80000 && time_binner.bins_ready_count() < coord.bin_count() as usize {
        let n1 = time_binner.bins_ready_count();
        if false {
            trace!(
                "events extra cycle {}  {}  {}",
                i,
                time_binner.bins_ready_count(),
                coord.bin_count()
            );
        }
        time_binner.cycle();
        i += 1;
        if time_binner.bins_ready_count() <= n1 {
            warn!("events cycle did not add another bin, break");
            break;
        }
    }
    if time_binner.bins_ready_count() < coord.bin_count() as usize {
        return Err(Error::with_msg_no_trace(format!(
            "events unable to produce all bins for the patch  bins_ready {}  coord.bin_count {}  edges.len {}",
            time_binner.bins_ready_count(),
            coord.bin_count(),
            edges.len(),
        )));
    }
    let ready = time_binner
        .bins_ready()
        .ok_or_else(|| Error::with_msg_no_trace(format!("unable to produce any bins for the patch")))?;
    if let Err(msg) = ready.validate() {
        error!("time binned invalid {}  coord {:?}", msg, coord);
    }
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
    scy: Arc<ScySession>,
) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn TimeBinned>, Error>> + Send>>, Error> {
    trace!("pre_binned_value_stream  series {series}  {chn:?}  {coord:?}");
    let res = pre_binned_value_stream_with_scy(series, chn, coord, agg_kind, cache_usage, scy).await?;
    error!("TODO pre_binned_value_stream");
    err::todo();
    Ok(Box::pin(futures_util::stream::iter([Ok(res.0)])))
}
