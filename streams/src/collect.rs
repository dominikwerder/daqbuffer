use err::Error;
use futures_util::{Stream, StreamExt};
use items::{RangeCompletableItem, Sitemty, StreamItem};
use items_0::collect_c::{Collectable, Collector};
use netpod::log::*;
use std::fmt;
use std::time::{Duration, Instant};
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

pub async fn collect<T, S>(
    stream: S,
    deadline: Instant,
    events_max: u64,
) -> Result<<<T as Collectable>::Collector as Collector>::Output, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable + fmt::Debug,
{
    let span = tracing::span!(tracing::Level::TRACE, "collect");
    let fut = async {
        let mut collector: Option<<T as Collectable>::Collector> = None;
        let mut stream = stream;
        let deadline = deadline.into();
        let mut range_complete = false;
        let mut total_duration = Duration::ZERO;
        loop {
            let item = match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(Some(k)) => k,
                Ok(None) => break,
                Err(_e) => {
                    if let Some(coll) = collector.as_mut() {
                        coll.set_timed_out();
                    } else {
                        warn!("Timeout but no collector yet");
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
                                warn!("Received RangeComplete but no collector yet");
                            }
                        }
                        RangeCompletableItem::Data(mut item) => {
                            if collector.is_none() {
                                let c = item.new_collector();
                                collector = Some(c);
                            }
                            let coll = collector.as_mut().unwrap();
                            coll.ingest(&mut item);
                            if coll.len() as u64 >= events_max {
                                warn!("Reached events_max {} abort", events_max);
                                break;
                            }
                        }
                    },
                    StreamItem::Log(item) => {
                        trace!("Log {:?}", item);
                    }
                    StreamItem::Stats(item) => {
                        trace!("Stats {:?}", item);
                        use items::StatsItem;
                        use netpod::DiskStats;
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
        let res = collector
            .ok_or_else(|| Error::with_msg_no_trace(format!("no result because no collector was created")))?
            .result()?;
        debug!("Total duration: {:?}", total_duration);
        Ok(res)
    };
    fut.instrument(span).await
}
