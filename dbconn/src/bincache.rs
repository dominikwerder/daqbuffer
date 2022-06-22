use crate::events_scylla::ScyllaFramableStream;
use crate::ErrConv;
use err::Error;
use futures_util::{Future, Stream, StreamExt};
use items::TimeBinned;
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::{
    AggKind, ChannelTyped, NanoRange, PreBinnedPatchCoord, PreBinnedPatchIterator, PreBinnedPatchRange, ScyllaConfig,
};
use scylla::Session as ScySession;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
        todo!()
    }
}

pub fn write_cached_scylla<'a>(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    data: &'a dyn TimeBinned,
    scy: &ScySession,
) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
    let chn = unsafe { &*(chn as *const ChannelTyped) };
    let data = unsafe { &*(data as *const dyn TimeBinned) };
    let scy = unsafe { &*(scy as *const ScySession) };
    let fut = async move {
        let _ = coord;
        let series = chn.series_id()?;
        let res = scy
            .query_iter("", (series as i64, data.dummy_test_i32()))
            .await
            .err_conv()?;
        let _ = res;
        // TODO write the data.
        //err::todoval();
        Ok(())
    };
    Box::pin(fut)
}

// TODO must indicate to the caller whether it is safe to cache this (complete).
pub async fn fetch_uncached_data(
    chn: ChannelTyped,
    coord: PreBinnedPatchCoord,
    scy: Arc<ScySession>,
) -> Result<Option<Box<dyn TimeBinned>>, Error> {
    info!("fetch_uncached_data");
    let range = coord.patch_range();
    // TODO why the extra plus one?
    let bin = match PreBinnedPatchRange::covering_range(range, coord.bin_count() + 1) {
        Ok(Some(range)) => fetch_uncached_higher_res_prebinned(&chn, &range, scy.clone()).await,
        Ok(None) => fetch_uncached_binned_events(&chn, &coord.patch_range(), scy.clone()).await,
        Err(e) => Err(e),
    }?;
    //let data = bin.workaround_clone();
    WriteFut::new(&chn, &coord, bin.as_ref(), &scy).await?;
    write_cached_scylla(&chn, &coord, bin.as_ref(), &scy).await?;
    Ok(Some(bin))
}

pub fn fetch_uncached_data_box(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    scy: Arc<ScySession>,
) -> Pin<Box<dyn Future<Output = Result<Option<Box<dyn TimeBinned>>, Error>> + Send>> {
    Box::pin(fetch_uncached_data(chn.clone(), coord.clone(), scy))
}

pub async fn fetch_uncached_higher_res_prebinned(
    chn: &ChannelTyped,
    range: &PreBinnedPatchRange,
    scy: Arc<ScySession>,
) -> Result<Box<dyn TimeBinned>, Error> {
    let mut aggt = None;
    let patch_it = PreBinnedPatchIterator::from_range(range.clone());
    for patch in patch_it {
        let coord = PreBinnedPatchCoord::new(patch.bin_t_len(), patch.patch_t_len(), patch.ix());
        let mut stream = pre_binned_value_stream_with_scy(chn, &coord, scy.clone()).await?;
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
    scy: Arc<ScySession>,
) -> Result<Box<dyn TimeBinned>, Error> {
    // TODO ask Scylla directly, do not go through HTTP.
    // Refactor the event fetch stream code such that I can use that easily here.
    let evq = RawEventsQuery::new(chn.channel.clone(), range.clone(), AggKind::Plain);
    let _res = Box::pin(ScyllaFramableStream::new(
        &evq,
        chn.scalar_type.clone(),
        chn.shape.clone(),
        scy,
        false,
    ));
    // TODO ScyllaFramableStream must return a new events trait object designed for trait object use.
    err::todoval()
}

pub async fn pre_binned_value_stream_with_scy(
    chn: &ChannelTyped,
    coord: &PreBinnedPatchCoord,
    scy: Arc<ScySession>,
) -> Result<Pin<Box<dyn Stream<Item = Result<Box<dyn TimeBinned>, Error>> + Send>>, Error> {
    info!("pre_binned_value_stream_with_scy  {chn:?}  {coord:?}");
    // TODO determine the range:
    let range = err::todoval();
    if let Some(item) = read_cached_scylla(chn, &range, &scy).await? {
        Ok(Box::pin(futures_util::stream::iter([Ok(item)])))
    } else {
        let bin = fetch_uncached_data_box(chn, coord, scy).await?;
        // TODO when can it ever be that we get back a None?
        // TODO also, probably the caller wants to know whether the bin is Complete.
        let bin = bin.unwrap();
        Ok(Box::pin(futures_util::stream::iter([Ok(bin)])))
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
    let scy = Arc::new(scy);
    pre_binned_value_stream_with_scy(chn, coord, scy).await
}
