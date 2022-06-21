use crate::ErrConv;
use err::Error;
use futures_util::{Future, Stream, StreamExt};
use items::TimeBinned;
use netpod::log::*;
use netpod::{ChannelTyped, NanoRange, PreBinnedPatchCoord, PreBinnedPatchIterator, PreBinnedPatchRange, ScyllaConfig};
use scylla::Session as ScySession;
use std::pin::Pin;

pub async fn read_cached_scylla(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    scy: &ScySession,
) -> Result<Option<Box<dyn TimeBinned>>, Error> {
    let _ = coord;
    let series = chn.series_id()?;
    let res = scy.query_iter("", (series as i64,)).await.err_conv()?;
    let _ = res;
    // TODO look for the data. Based on the ChannelTyped we know what type the caller expects.
    err::todoval()
}

pub async fn write_cached_scylla(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    data: &dyn TimeBinned,
    scy: &ScySession,
) -> Result<(), Error> {
    let _ = coord;
    let _ = data;
    let series = chn.series_id()?;
    let res = scy.query_iter("", (series as i64,)).await.err_conv()?;
    let _ = res;
    // TODO write the data.
    err::todoval()
}

// TODO must indicate to the caller whether it is safe to cache this (complete).
pub async fn fetch_uncached_data(
    chn: ChannelTyped,
    coord: PreBinnedPatchCoord,
    scy: &ScySession,
) -> Result<Option<Box<dyn TimeBinned>>, Error> {
    info!("fetch_uncached_data");
    let range = coord.patch_range();
    // TODO why the extra plus one?
    let bin = match PreBinnedPatchRange::covering_range(range, coord.bin_count() + 1) {
        Ok(Some(range)) => fetch_uncached_higher_res_prebinned(&chn, &range, scy).await,
        Ok(None) => fetch_uncached_binned_events(&chn, &coord.patch_range(), scy).await,
        Err(e) => Err(e),
    }?;
    err::todoval()
}

pub fn fetch_uncached_data_box(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    scy: &ScySession,
) -> Pin<Box<dyn Future<Output = Result<Option<Box<dyn TimeBinned>>, Error>> + Send>> {
    let scy = unsafe { &*(scy as *const _) };
    Box::pin(fetch_uncached_data(chn.clone(), coord.clone(), scy))
}

pub async fn fetch_uncached_higher_res_prebinned(
    chn: &ChannelTyped,
    range: &PreBinnedPatchRange,
    scy: &ScySession,
) -> Result<Box<dyn TimeBinned>, Error> {
    let mut aggt = None;
    let patch_it = PreBinnedPatchIterator::from_range(range.clone());
    for patch in patch_it {
        let coord = PreBinnedPatchCoord::new(patch.bin_t_len(), patch.patch_t_len(), patch.ix());
        let mut stream = pre_binned_value_stream_with_scy(chn, &coord, scy).await?;
        while let Some(item) = stream.next().await {
            let item = item?;
            // TODO here I will need some new API to aggregate (time-bin) trait objects.
            // Each TimeBinned must provide some way to do that...
            // I also need an Aggregator which does not know before the first item what output type it will produce.
            let _ = item;
            if aggt.is_none() {
                aggt = Some(item.aggregator_new());
            }
            let aggt = aggt.as_mut().unwrap();
            aggt.ingest(item.as_time_binnable_dyn());
        }
    }
    let mut aggt = aggt.unwrap();
    let res = aggt.result();
    Ok(res)
}

pub async fn fetch_uncached_binned_events(
    chn: &ChannelTyped,
    range: &NanoRange,
    scy: &ScySession,
) -> Result<Box<dyn TimeBinned>, Error> {
    // TODO ask Scylla directly, do not go through HTTP.
    // Refactor the event fetch stream code such that I can use that easily here.
    err::todoval()
}

pub async fn pre_binned_value_stream_with_scy(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    scy: &ScySession,
) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn TimeBinned>, Error>> + Send>>, Error> {
    info!("pre_binned_value_stream_with_scy  {chn:?}  {coord:?}");
    let range = err::todoval();
    if let Some(item) = read_cached_scylla(chn, &range, &scy).await? {
        Ok(Box::pin(futures_util::stream::iter([Ok(item)])))
    } else {
        let bin = fetch_uncached_data_box(chn, coord, scy).await?;
        Ok(Box::pin(futures_util::stream::empty()))
    }
}

pub async fn pre_binned_value_stream(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    scyconf: &ScyllaConfig,
) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn TimeBinned>, Error>> + Send>>, Error> {
    info!("pre_binned_value_stream  {chn:?}  {coord:?}  {scyconf:?}");
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .use_keyspace(&scyconf.keyspace, true)
        .build()
        .await
        .err_conv()?;
    pre_binned_value_stream_with_scy(chn, coord, &scy).await
}
