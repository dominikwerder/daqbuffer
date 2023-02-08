pub mod binsdim0;
pub mod binsxbindim0;
pub mod channelevents;
pub mod databuffereventblobs;
pub mod eventsdim0;
pub mod eventsdim1;
pub mod eventsxbindim0;
pub mod merger;
pub mod merger_cev;
pub mod streams;
#[cfg(test)]
pub mod test;
pub mod testgen;
pub mod timebin;

use channelevents::ChannelEvents;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items::RangeCompletableItem;
use items::Sitemty;
use items::StreamItem;
use items_0::collect_s::Collector;
use items_0::collect_s::ToJsonResult;
use items_0::Empty;
use items_0::Events;
use items_0::RangeOverlapInfo;
use items_0::TimeBinnable;
use items_0::TimeBinner;
use netpod::log::*;
use netpod::timeunits::*;
use netpod::AggKind;
use netpod::NanoRange;
use netpod::ScalarType;
use netpod::Shape;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

pub fn bool_is_false(x: &bool) -> bool {
    *x == false
}

pub fn ts_offs_from_abs(tss: &[u64]) -> (u64, VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_sec = tss.first().map_or(0, |&k| k) / SEC;
    info!("ts_offs_from_abs  ts_anchor_sec {ts_anchor_sec}");
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: VecDeque<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_anchor_sec, ts_off_ms, ts_off_ns)
}

pub fn ts_offs_from_abs_with_anchor(ts_anchor_sec: u64, tss: &[u64]) -> (VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: VecDeque<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_off_ms, ts_off_ns)
}

pub fn pulse_offs_from_abs(pulse: &[u64]) -> (u64, VecDeque<u64>) {
    error!("pulse_offs_from_abs  {} DATA", pulse.len());
    for x in pulse {
        error!("{x}");
    }
    let pulse_anchor = pulse.first().map_or(0, |&k| k) / 10000 * 10000;
    info!("pulse_offs_from_abs  pulse_anchor {pulse_anchor}");
    let pulse_off = pulse.iter().map(|&k| k - pulse_anchor).collect();
    (pulse_anchor, pulse_off)
}

#[allow(unused)]
struct Ts(u64);

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    General,
    #[allow(unused)]
    MismatchedType,
}

// TODO stack error better
#[derive(Debug, PartialEq)]
pub struct Error {
    #[allow(unused)]
    kind: ErrorKind,
    msg: Option<String>,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind, msg: None }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Self {
            msg: Some(msg),
            kind: ErrorKind::General,
        }
    }
}

// TODO this discards structure
impl From<err::Error> for Error {
    fn from(e: err::Error) -> Self {
        Self {
            msg: Some(format!("{e}")),
            kind: ErrorKind::General,
        }
    }
}

// TODO this discards structure
impl From<Error> for err::Error {
    fn from(e: Error) -> Self {
        err::Error::with_msg_no_trace(format!("{e}"))
    }
}

impl std::error::Error for Error {}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        format!("{msg}").into()
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct IsoDateTime(DateTime<Utc>);

impl IsoDateTime {
    pub fn from_u64(ts: u64) -> Self {
        IsoDateTime(Utc.timestamp_nanos(ts as i64))
    }
}

impl Serialize for IsoDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string())
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect()
}

impl crate::merger::Mergeable for Box<dyn Events> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn ts_min(&self) -> Option<u64> {
        self.as_ref().ts_min()
    }

    fn ts_max(&self) -> Option<u64> {
        self.as_ref().ts_max()
    }

    fn is_compatible_target(&self, _tgt: &Self) -> bool {
        // TODO currently unused
        todo!()
    }

    fn move_into_fresh(&mut self, ts_end: u64) -> Self {
        self.as_mut().move_into_fresh(ts_end)
    }

    fn move_into_existing(&mut self, tgt: &mut Self, ts_end: u64) -> Result<(), merger::MergeError> {
        self.as_mut()
            .move_into_existing(tgt, ts_end)
            .map_err(|()| merger::MergeError::NotCompatible)
    }
}

// TODO rename to `Typed`
pub trait TimeBinnableType: Send + Unpin + RangeOverlapInfo + Empty {
    type Output: TimeBinnableType;
    type Aggregator: TimeBinnableTypeAggregator<Input = Self, Output = Self::Output> + Send + Unpin;
    fn aggregator(range: NanoRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator;
}

pub trait TimeBinnableTypeAggregator: Send {
    type Input: TimeBinnableType;
    type Output: TimeBinnableType;
    fn range(&self) -> &NanoRange;
    fn ingest(&mut self, item: &Self::Input);
    fn result_reset(&mut self, range: NanoRange, expand: bool) -> Self::Output;
}

pub fn empty_events_dyn_ev(
    scalar_type: &ScalarType,
    shape: &Shape,
    agg_kind: &AggKind,
) -> Result<Box<dyn Events>, Error> {
    let ret: Box<dyn Events> = match shape {
        Shape::Scalar => match agg_kind {
            AggKind::Plain | AggKind::TimeWeightedScalar => {
                use ScalarType::*;
                type K<T> = eventsdim0::EventsDim0<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    U16 => Box::new(K::<u16>::empty()),
                    U32 => Box::new(K::<u32>::empty()),
                    U64 => Box::new(K::<u64>::empty()),
                    I8 => Box::new(K::<i8>::empty()),
                    I16 => Box::new(K::<i16>::empty()),
                    I32 => Box::new(K::<i32>::empty()),
                    I64 => Box::new(K::<i64>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    BOOL => Box::new(K::<bool>::empty()),
                    STRING => Box::new(K::<String>::empty()),
                }
            }
            _ => {
                error!("TODO empty_events_dyn_2  {scalar_type:?}  {shape:?}  {agg_kind:?}");
                err::todoval()
            }
        },
        Shape::Wave(..) => match agg_kind {
            AggKind::Plain => {
                use ScalarType::*;
                type K<T> = eventsdim1::EventsDim1<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    U16 => Box::new(K::<u16>::empty()),
                    U32 => Box::new(K::<u32>::empty()),
                    U64 => Box::new(K::<u64>::empty()),
                    I8 => Box::new(K::<i8>::empty()),
                    I16 => Box::new(K::<i16>::empty()),
                    I32 => Box::new(K::<i32>::empty()),
                    I64 => Box::new(K::<i64>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    BOOL => Box::new(K::<bool>::empty()),
                    STRING => Box::new(K::<String>::empty()),
                }
            }
            _ => {
                error!("TODO empty_events_dyn_2  {scalar_type:?}  {shape:?}  {agg_kind:?}");
                err::todoval()
            }
        },
        Shape::Image(..) => {
            error!("TODO empty_events_dyn_2  {scalar_type:?}  {shape:?}  {agg_kind:?}");
            err::todoval()
        }
    };
    Ok(ret)
}

pub fn empty_binned_dyn_tb(scalar_type: &ScalarType, shape: &Shape, agg_kind: &AggKind) -> Box<dyn TimeBinnable> {
    match shape {
        Shape::Scalar => match agg_kind {
            AggKind::TimeWeightedScalar => {
                use ScalarType::*;
                type K<T> = binsdim0::BinsDim0<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    U16 => Box::new(K::<u16>::empty()),
                    U32 => Box::new(K::<u32>::empty()),
                    U64 => Box::new(K::<u64>::empty()),
                    I8 => Box::new(K::<i8>::empty()),
                    I16 => Box::new(K::<i16>::empty()),
                    I32 => Box::new(K::<i32>::empty()),
                    I64 => Box::new(K::<i64>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    _ => {
                        error!("TODO empty_binned_dyn");
                        err::todoval()
                    }
                }
            }
            _ => {
                error!("TODO empty_binned_dyn");
                err::todoval()
            }
        },
        Shape::Wave(_n) => match agg_kind {
            AggKind::DimXBins1 => {
                use ScalarType::*;
                type K<T> = binsdim0::BinsDim0<T>;
                match scalar_type {
                    U8 => Box::new(K::<u8>::empty()),
                    F32 => Box::new(K::<f32>::empty()),
                    F64 => Box::new(K::<f64>::empty()),
                    _ => {
                        error!("TODO empty_binned_dyn");
                        err::todoval()
                    }
                }
            }
            _ => {
                error!("TODO empty_binned_dyn");
                err::todoval()
            }
        },
        Shape::Image(..) => {
            error!("TODO empty_binned_dyn");
            err::todoval()
        }
    }
}

fn flush_binned(
    binner: &mut Box<dyn TimeBinner>,
    coll: &mut Option<Box<dyn Collector>>,
    force: bool,
) -> Result<(), Error> {
    trace!("flush_binned  bins_ready_count: {}", binner.bins_ready_count());
    if force {
        if binner.bins_ready_count() == 0 {
            debug!("cycle the binner forced");
            binner.cycle();
        } else {
            debug!("bins ready, do not force");
        }
    }
    if binner.bins_ready_count() > 0 {
        let ready = binner.bins_ready();
        match ready {
            Some(mut ready) => {
                trace!("binned_collected ready {ready:?}");
                if coll.is_none() {
                    *coll = Some(ready.as_collectable_mut().new_collector());
                }
                let cl = coll.as_mut().unwrap();
                cl.ingest(ready.as_collectable_mut());
                Ok(())
            }
            None => Err(format!("bins_ready_count but no result").into()),
        }
    } else {
        Ok(())
    }
}

// TODO remove.
// Compare with items_2::test::bin01
pub async fn binned_collected(
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
    edges: Vec<u64>,
    timeout: Duration,
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
) -> Result<Box<dyn ToJsonResult>, Error> {
    event!(Level::TRACE, "binned_collected");
    if edges.len() < 2 {
        return Err(format!("binned_collected  but edges.len() {}", edges.len()).into());
    }
    let ts_edges_max = *edges.last().unwrap();
    let deadline = Instant::now() + timeout;
    let mut did_timeout = false;
    // TODO use a trait to allow check of unfinished data [hcn2956jxhwsf]
    #[allow(unused)]
    let bin_count_exp = edges.len().max(2) as u32 - 1;
    let do_time_weight = agg_kind.do_time_weighted();
    // TODO maybe TimeBinner should take all ChannelEvents and handle this?
    let mut did_range_complete = false;
    let mut coll = None;
    let mut binner = None;
    let empty_item = empty_events_dyn_ev(&scalar_type, &shape, &AggKind::TimeWeightedScalar)?;
    let tmp_item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(ChannelEvents::Events(
        empty_item,
    ))));
    let empty_stream = futures_util::stream::once(futures_util::future::ready(tmp_item));
    let mut stream = empty_stream.chain(inp);
    loop {
        let item = futures_util::select! {
            k = stream.next().fuse() => {
                if let Some(k) = k {
                    k?
                }else {
                    break;
                }
            },
            _ = tokio::time::sleep_until(deadline.into()).fuse() => {
                did_timeout = true;
                break;
            }
        };
        match item {
            StreamItem::DataItem(k) => match k {
                RangeCompletableItem::RangeComplete => {
                    did_range_complete = true;
                }
                RangeCompletableItem::Data(k) => match k {
                    ChannelEvents::Events(events) => {
                        if events.starts_after(NanoRange {
                            beg: 0,
                            end: ts_edges_max,
                        }) {
                        } else {
                            if binner.is_none() {
                                let bb = events.as_time_binnable().time_binner_new(edges.clone(), do_time_weight);
                                binner = Some(bb);
                            }
                            let binner = binner.as_mut().unwrap();
                            binner.ingest(events.as_time_binnable());
                            flush_binned(binner, &mut coll, false)?;
                        }
                    }
                    ChannelEvents::Status(item) => {
                        trace!("{:?}", item);
                    }
                },
            },
            StreamItem::Log(item) => {
                // TODO collect also errors here?
                trace!("{:?}", item);
            }
            StreamItem::Stats(item) => {
                // TODO do something with the stats
                trace!("{:?}", item);
            }
        }
    }
    if let Some(mut binner) = binner {
        if did_range_complete {
            trace!("did_range_complete");
            binner.set_range_complete();
        } else {
            debug!("range not complete");
        }
        if did_timeout {
            warn!("timeout");
        } else {
            trace!("cycle the binner");
            binner.cycle();
        }
        trace!("flush binned");
        flush_binned(&mut binner, &mut coll, false)?;
        if coll.is_none() {
            debug!("force a bin");
            flush_binned(&mut binner, &mut coll, true)?;
        } else {
            trace!("coll is already some");
        }
    } else {
        error!("no binner, should always have one");
    }
    match coll {
        Some(mut coll) => {
            let res = coll.result(None, None).map_err(|e| format!("{e}"))?;
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Ok(res)
        }
        None => {
            error!("binned_collected nothing collected");
            let item = empty_binned_dyn_tb(&scalar_type, &shape, &AggKind::DimXBins1);
            let ret = item.to_box_to_json_result();
            tokio::time::sleep(Duration::from_millis(2000)).await;
            Ok(ret)
        }
    }
}

pub fn runfut<T, F>(fut: F) -> Result<T, err::Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    use futures_util::TryFutureExt;
    let fut = fut.map_err(|e| e.into());
    taskrun::run(fut)
}
