use crate::RangeCompletableItem;
use crate::StreamItem;
use crate::WithLen;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use netpod::log::*;
use netpod::DiskStats;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::fmt;
use std::time::Duration;
use tokio::time::timeout_at;

pub trait Collector: Send + Unpin + WithLen {
    type Input: Collectable;
    type Output: Serialize;
    fn ingest(&mut self, src: &Self::Input);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn result(self) -> Result<Self::Output, Error>;
}

pub trait Collectable {
    type Collector: Collector<Input = Self>;
    fn new_collector(bin_count_exp: u32) -> Self::Collector;
}

pub trait ToJsonBytes {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error>;
}

pub trait ToJsonResult {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error>;
}

impl ToJsonBytes for serde_json::Value {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}

impl ToJsonResult for Sitemty<serde_json::Value> {
    fn to_json_result(&self) -> Result<Box<dyn ToJsonBytes>, Error> {
        match self {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::Data(item) => Ok(Box::new(item.clone())),
                    RangeCompletableItem::RangeComplete => Err(Error::with_msg("RangeComplete")),
                },
                StreamItem::Log(item) => Err(Error::with_msg(format!("Log {:?}", item))),
                StreamItem::Stats(item) => Err(Error::with_msg(format!("Stats {:?}", item))),
            },
            Err(e) => Err(Error::with_msg(format!("Error {:?}", e))),
        }
    }
}

// TODO rename, it is also used for binned:
pub async fn collect_plain_events_json<T, S>(
    stream: S,
    timeout: Duration,
    bin_count_exp: u32,
    events_max: u64,
    do_log: bool,
) -> Result<JsonValue, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: Collectable + fmt::Debug,
{
    let deadline = tokio::time::Instant::now() + timeout;
    // TODO in general a Collector does not need to know about the expected number of bins.
    // It would make more sense for some specific Collector kind to know.
    // Therefore introduce finer grained types.
    let mut collector = <T as Collectable>::new_collector(bin_count_exp);
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
                match timeout_at(deadline, stream.next()).await {
                    Ok(k) => k,
                    Err(_) => {
                        collector.set_timed_out();
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
                            if do_log {
                                debug!("collect_plain_events_json log {:?}", item);
                            }
                        }
                        StreamItem::Stats(item) => {
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
                                collector.set_range_complete();
                            }
                            RangeCompletableItem::Data(item) => {
                                collector.ingest(&item);
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
    let ret = serde_json::to_value(collector.result()?)?;
    debug!("Total duration: {:?}", total_duration);
    Ok(ret)
}
