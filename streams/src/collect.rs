use err::Error;
use futures_util::Future;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::collect_s::Collected;
use items_0::collect_s::Collector;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use items_0::transform::CollectableStreamBox;
use items_0::transform::CollectableStreamTrait;
use items_0::transform::EventStreamTrait;
use items_0::WithLen;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRangeEnum;
use netpod::DiskStats;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use tracing::Instrument;

#[allow(unused)]
macro_rules! trace2 {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

#[allow(unused)]
macro_rules! trace3 {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

#[allow(unused)]
macro_rules! trace4 {
    (D$($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

pub struct Collect {
    inp: CollectableStreamBox,
    deadline: Instant,
}

impl Collect {
    pub fn new<INP>(inp: INP, deadline: Instant) -> Self
    where
        INP: CollectableStreamTrait + 'static,
    {
        Self {
            inp: CollectableStreamBox(Box::pin(inp)),
            deadline,
        }
    }
}

impl Future for Collect {
    type Output = Sitemty<Box<dyn Collected>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let span = tracing::span!(Level::INFO, "Collect");
        let _spg = span.enter();
        todo!()
    }
}

async fn collect_in_span<T, S>(
    stream: S,
    deadline: Instant,
    events_max: u64,
    range: Option<SeriesRange>,
    binrange: Option<BinnedRangeEnum>,
) -> Result<Box<dyn Collected>, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable + WithLen + fmt::Debug,
{
    info!("collect  events_max {events_max}  deadline {deadline:?}");
    let mut collector: Option<Box<dyn Collector>> = None;
    let mut stream = stream;
    let deadline = deadline.into();
    let mut range_complete = false;
    let mut timed_out = false;
    let mut total_duration = Duration::ZERO;
    loop {
        let item = match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(k)) => k,
            Ok(None) => break,
            Err(_e) => {
                warn!("collect timeout");
                timed_out = true;
                if let Some(coll) = collector.as_mut() {
                    coll.set_timed_out();
                } else {
                    warn!("collect timeout but no collector yet");
                }
                break;
            }
        };
        match item {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {
                        range_complete = true;
                        if let Some(coll) = collector.as_mut() {
                            coll.set_range_complete();
                        } else {
                            warn!("collect received RangeComplete but no collector yet");
                        }
                    }
                    RangeCompletableItem::Data(mut item) => {
                        info!("collect sees len {}", item.len());
                        if collector.is_none() {
                            let c = item.new_collector();
                            collector = Some(c);
                        }
                        let coll = collector.as_mut().unwrap();
                        coll.ingest(&mut item);
                        if coll.len() as u64 >= events_max {
                            warn!("TODO  compute continue-at  reached events_max {} abort", events_max);
                            break;
                        }
                    }
                },
                StreamItem::Log(item) => {
                    trace!("collect log {:?}", item);
                }
                StreamItem::Stats(item) => {
                    trace!("collect stats {:?}", item);
                    match item {
                        // TODO factor and simplify the stats collection:
                        StatsItem::EventDataReadStats(_) => {}
                        StatsItem::RangeFilterStats(_) => {}
                        StatsItem::DiskStats(item) => match item {
                            DiskStats::OpenStats(k) => {
                                total_duration += k.duration;
                            }
                            DiskStats::SeekStats(k) => {
                                total_duration += k.duration;
                            }
                            DiskStats::ReadStats(k) => {
                                total_duration += k.duration;
                            }
                            DiskStats::ReadExactStats(k) => {
                                total_duration += k.duration;
                            }
                        },
                    }
                }
            },
            Err(e) => {
                // TODO  Need to use some flags to get good enough error message for remote user.
                return Err(e);
            }
        }
    }
    let _ = range_complete;
    let _ = timed_out;
    let res = collector
        .ok_or_else(|| Error::with_msg_no_trace(format!("no result because no collector was created")))?
        .result(range, binrange)?;
    info!("collect stats total duration: {:?}", total_duration);
    Ok(res)
}

pub async fn collect<T, S>(
    stream: S,
    deadline: Instant,
    events_max: u64,
    range: Option<SeriesRange>,
    binrange: Option<BinnedRangeEnum>,
) -> Result<Box<dyn Collected>, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable + WithLen + fmt::Debug,
{
    let span = span!(Level::INFO, "collect");
    collect_in_span(stream, deadline, events_max, range, binrange)
        .instrument(span)
        .await
}
