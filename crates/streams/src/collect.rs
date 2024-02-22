use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::Collectable;
use items_0::collect_s::Collected;
use items_0::collect_s::Collector;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
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
    inp: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>>,
    events_max: u64,
    range: Option<SeriesRange>,
    binrange: Option<BinnedRangeEnum>,
    collector: Option<Box<dyn Collector>>,
    range_final: bool,
    timeout: bool,
    timer: Pin<Box<dyn Future<Output = ()> + Send>>,
    done_input: bool,
}

impl Collect {
    pub fn new(
        inp: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Collectable>>> + Send>>,
        deadline: Instant,
        events_max: u64,
        range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Self {
        let timer = tokio::time::sleep_until(deadline.into());
        Self {
            inp,
            events_max,
            range,
            binrange,
            collector: None,
            range_final: false,
            timeout: false,
            timer: Box::pin(timer),
            done_input: false,
        }
    }

    fn handle_item(&mut self, item: Sitemty<Box<dyn Collectable>>) -> Result<(), Error> {
        match item {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {
                        self.range_final = true;
                        if let Some(coll) = self.collector.as_mut() {
                            coll.set_range_complete();
                        } else {
                            warn!("collect received RangeComplete but no collector yet");
                        }
                        Ok(())
                    }
                    RangeCompletableItem::Data(mut item) => {
                        trace!("collect sees len {}", item.len());
                        let coll = self.collector.get_or_insert_with(|| item.new_collector());
                        coll.ingest(&mut item);
                        if coll.len() as u64 >= self.events_max {
                            info!("reached events_max {}", self.events_max);
                            coll.set_continue_at_here();
                            self.done_input = true;
                        }
                        Ok(())
                    }
                },
                StreamItem::Log(item) => {
                    if item.level == Level::ERROR {
                        error!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::WARN {
                        warn!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::INFO {
                        info!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::DEBUG {
                        debug!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::TRACE {
                        trace!("node {}  msg {}", item.node_ix, item.msg);
                    }
                    Ok(())
                }
                StreamItem::Stats(item) => {
                    trace!("collect stats {:?}", item);
                    match item {
                        // TODO factor and simplify the stats collection:
                        StatsItem::EventDataReadStats(_) => {}
                        StatsItem::RangeFilterStats(_) => {}
                        StatsItem::DiskStats(item) => match item {
                            DiskStats::OpenStats(_) => {
                                //total_duration += k.duration;
                            }
                            DiskStats::SeekStats(_) => {
                                //total_duration += k.duration;
                            }
                            DiskStats::ReadStats(_) => {
                                //total_duration += k.duration;
                            }
                            DiskStats::ReadExactStats(_) => {
                                //total_duration += k.duration;
                            }
                        },
                        _ => {}
                    }
                    Ok(())
                }
            },
            Err(e) => {
                // TODO  Need to use some flags to get good enough error message for remote user.
                Err(e)
            }
        }
    }
}

impl Future for Collect {
    type Output = Result<Box<dyn Collected>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let span = tracing::span!(Level::INFO, "Collect");
        let _spg = span.enter();
        loop {
            break if self.done_input {
                if self.timeout {
                    if let Some(coll) = self.collector.as_mut() {
                        coll.set_timed_out();
                    } else {
                        warn!("collect timeout but no collector yet");
                    }
                }
                // TODO use range_final and timeout in result.
                match self.collector.take() {
                    Some(mut coll) => match coll.result(self.range.clone(), self.binrange.clone()) {
                        Ok(res) => {
                            //info!("collect stats total duration: {:?}", total_duration);
                            Ready(Ok(res))
                        }
                        Err(e) => Ready(Err(e)),
                    },
                    None => {
                        let e = Error::with_msg_no_trace(format!("no result because no collector was created"));
                        error!("{e}");
                        Ready(Err(e))
                    }
                }
            } else {
                match self.timer.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = true;
                        self.done_input = true;
                        continue;
                    }
                    Pending => match self.inp.poll_next_unpin(cx) {
                        Ready(Some(item)) => match self.handle_item(item) {
                            Ok(()) => {
                                continue;
                            }
                            Err(e) => {
                                error!("{e}");
                                Ready(Err(e))
                            }
                        },
                        Ready(None) => {
                            self.done_input = true;
                            continue;
                        }
                        Pending => Pending,
                    },
                }
            };
        }
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
    T: Collectable,
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
                        trace!("collect sees len {}", item.len());
                        if collector.is_none() {
                            let c = item.new_collector();
                            collector = Some(c);
                        }
                        let coll = collector.as_mut().unwrap();
                        coll.ingest(&mut item);
                        if coll.len() as u64 >= events_max {
                            warn!("span reached events_max {}", events_max);
                            coll.set_continue_at_here();
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
                        _ => {}
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
