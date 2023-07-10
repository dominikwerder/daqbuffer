pub mod binnedcollected;
pub mod binsdim0;
pub mod binsxbindim0;
pub mod channelevents;
pub mod empty;
pub mod eventfull;
pub mod eventsdim0;
pub mod eventsdim1;
pub mod eventsxbindim0;
pub mod framable;
pub mod frame;
pub mod inmem;
pub mod merger;
pub mod streams;
#[cfg(test)]
pub mod test;
pub mod testgen;
pub mod timebin;
pub mod transform;

use channelevents::ChannelEvents;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use futures_util::Stream;
use items_0::overlap::RangeOverlapInfo;
use items_0::streamitem::Sitemty;
use items_0::transform::EventTransform;
use items_0::Empty;
use items_0::Events;
use items_0::MergeError;
use merger::Mergeable;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::*;
use netpod::DATETIME_FMT_3MS;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use std::collections::VecDeque;
use std::fmt;

pub fn ts_offs_from_abs(tss: &[u64]) -> (u64, VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_sec = tss.first().map_or(0, |&k| k) / SEC;
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
    let pulse_anchor = pulse.first().map_or(0, |&k| k) / 10000 * 10000;
    let pulse_off = pulse.iter().map(|&k| k - pulse_anchor).collect();
    (pulse_anchor, pulse_off)
}

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
        serializer.serialize_str(&self.0.format(DATETIME_FMT_3MS).to_string())
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect()
}

impl Mergeable for Box<dyn Events> {
    fn ts_min(&self) -> Option<u64> {
        self.as_ref().ts_min()
    }

    fn ts_max(&self) -> Option<u64> {
        self.as_ref().ts_max()
    }

    fn new_empty(&self) -> Self {
        self.as_ref().new_empty_evs()
    }

    fn drain_into(&mut self, dst: &mut Self, range: (usize, usize)) -> Result<(), MergeError> {
        self.as_mut().drain_into_evs(dst, range)
    }

    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_lowest_index_gt_evs(ts)
    }

    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_lowest_index_ge_evs(ts)
    }

    fn find_highest_index_lt(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_highest_index_lt_evs(ts)
    }
}

// TODO rename to `Typed`
pub trait TimeBinnableType: Send + Unpin + RangeOverlapInfo + Empty {
    type Output: TimeBinnableType;
    type Aggregator: TimeBinnableTypeAggregator<Input = Self, Output = Self::Output> + Send + Unpin;
    fn aggregator(range: SeriesRange, bin_count: usize, do_time_weight: bool) -> Self::Aggregator;
}

pub trait TimeBinnableTypeAggregator: Send {
    type Input: TimeBinnableType;
    type Output: TimeBinnableType;
    fn range(&self) -> &SeriesRange;
    fn ingest(&mut self, item: &Self::Input);
    fn result_reset(&mut self, range: SeriesRange) -> Self::Output;
}

pub trait ChannelEventsInput: Stream<Item = Sitemty<ChannelEvents>> + EventTransform + Send {}

impl<T> ChannelEventsInput for T where T: Stream<Item = Sitemty<ChannelEvents>> + EventTransform + Send {}

pub fn runfut<T, F>(fut: F) -> Result<T, err::Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    use futures_util::TryFutureExt;
    let fut = fut.map_err(|e| e.into());
    taskrun::run(fut)
}
