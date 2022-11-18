use err::Error;
use futures_util::{Stream, StreamExt};
use items::{RangeCompletableItem, Sitemty, StreamItem};
use items_2::streams::{Collectable, Collector};
use netpod::log::*;
use serde_json::Value as JsonValue;
use std::fmt;
use std::time::Duration;

// This is meant to work with trait object event containers (crate items_2)

// TODO rename, it is also used for binned:
pub async fn collect_plain_events_json<T, S>(stream: S, timeout: Duration, events_max: u64) -> Result<JsonValue, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable + fmt::Debug,
{
    let deadline = tokio::time::Instant::now() + timeout;
    // TODO in general a Collector does not need to know about the expected number of bins.
    // It would make more sense for some specific Collector kind to know.
    // Therefore introduce finer grained types.
    let mut collector: Option<Box<dyn Collector>> = None;
    let mut i1 = 0;
    let mut stream = stream;
    let mut total_duration = Duration::ZERO;
    loop {
        let item = if i1 == 0 {
            stream.next().await
        } else {
            if false {
                None
            } else {
                match tokio::time::timeout_at(deadline, stream.next()).await {
                    Ok(k) => k,
                    Err(_) => {
                        eprintln!("TODO [smc3j3rwha732ru8wcnfgi]");
                        err::todo();
                        //collector.set_timed_out();
                        None
                    }
                }
            }
        };
        match item {
            Some(item) => {
                match item {
                    Ok(item) => match item {
                        StreamItem::Log(item) => {
                            trace!("collect_plain_events_json log {:?}", item);
                        }
                        StreamItem::Stats(item) => {
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
                        StreamItem::DataItem(item) => match item {
                            RangeCompletableItem::RangeComplete => {
                                eprintln!("TODO [73jdfcgf947d]");
                                err::todo();
                                //collector.set_range_complete();
                            }
                            RangeCompletableItem::Data(item) => {
                                eprintln!("TODO [nx298nu98venusfc8]");
                                err::todo();
                                //collector.ingest(&item);
                                i1 += 1;
                                if i1 >= events_max {
                                    break;
                                }
                            }
                        },
                    },
                    Err(e) => {
                        // TODO  Need to use some flags to get good enough error message for remote user.
                        Err(e)?;
                    }
                };
            }
            None => break,
        }
    }
    let res = collector
        .ok_or_else(|| Error::with_msg_no_trace(format!("no collector created")))?
        .result()?;
    let ret = serde_json::to_value(&res)?;
    debug!("Total duration: {:?}", total_duration);
    Ok(ret)
}
