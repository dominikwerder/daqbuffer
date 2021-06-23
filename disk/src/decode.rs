use crate::agg::binnedt::TimeBinnableType;
use crate::agg::enp::{ts_offs_from_abs, Identity, WaveNBinner, WavePlainProc, WaveXBinner};
use crate::agg::streams::{Appendable, Collectable, Collector, StreamItem};
use crate::agg::{Fits, FitsInside};
use crate::binned::{
    Bool, EventValuesAggregator, EventsNodeProcessor, FilterFittingInside, MinMaxAvgBins, NumOps, PushableIndex,
    RangeCompletableItem, RangeOverlapInfo, ReadPbv, ReadableFromFile, WithLen, WithTimestamps,
};
use crate::eventblobs::EventBlobsComplete;
use crate::eventchunker::EventFull;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::NanoRange;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::mem::size_of;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::File;

pub trait Endianness: Send + Unpin {
    fn is_big() -> bool;
}
pub struct LittleEndian {}
pub struct BigEndian {}
impl Endianness for LittleEndian {
    fn is_big() -> bool {
        false
    }
}
impl Endianness for BigEndian {
    fn is_big() -> bool {
        true
    }
}

pub trait NumFromBytes<NTY, END> {
    fn convert(buf: &[u8], big_endian: bool) -> NTY;
}

macro_rules! impl_num_from_bytes_end {
    ($nty:ident, $nl:expr, $end:ident, $ec:ident) => {
        impl NumFromBytes<$nty, $end> for $nty {
            fn convert(buf: &[u8], big_endian: bool) -> $nty {
                // Error in data on disk:
                // Can not rely on byte order as stated in the channel config.
                //$nty::$ec(*arrayref::array_ref![buf, 0, $nl])
                if big_endian {
                    $nty::from_be_bytes(*arrayref::array_ref![buf, 0, $nl])
                } else {
                    $nty::from_le_bytes(*arrayref::array_ref![buf, 0, $nl])
                }
            }
        }
    };
}

macro_rules! impl_num_from_bytes {
    ($nty:ident, $nl:expr) => {
        impl_num_from_bytes_end!($nty, $nl, LittleEndian, from_le_bytes);
        impl_num_from_bytes_end!($nty, $nl, BigEndian, from_be_bytes);
    };
}

impl_num_from_bytes!(u8, 1);
impl_num_from_bytes!(u16, 2);
impl_num_from_bytes!(u32, 4);
impl_num_from_bytes!(u64, 8);
impl_num_from_bytes!(i8, 1);
impl_num_from_bytes!(i16, 2);
impl_num_from_bytes!(i32, 4);
impl_num_from_bytes!(i64, 8);
impl_num_from_bytes!(f32, 4);
impl_num_from_bytes!(f64, 8);

pub trait EventValueFromBytes<NTY, END>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output;
    // The written data on disk has errors:
    // The endian as stated in the channel config does not match written events.
    // Therefore, can not rely on that but have to check for each single event...
    fn convert(&self, buf: &[u8], big_endian: bool) -> Result<Self::Output, Error>;
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim0Case<NTY>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output = NTY;

    fn convert(&self, buf: &[u8], big_endian: bool) -> Result<Self::Output, Error> {
        Ok(NTY::convert(buf, big_endian))
    }
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim1Case<NTY>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output = Vec<NTY>;

    fn convert(&self, buf: &[u8], big_endian: bool) -> Result<Self::Output, Error> {
        let es = size_of::<NTY>();
        let n1 = buf.len() / es;
        if n1 != self.n as usize {
            return Err(Error::with_msg(format!("ele count  got {}  exp {}", n1, self.n)));
        }
        let mut vals = vec![];
        // TODO could optimize using unsafe code..
        for n2 in 0..n1 {
            let i1 = es * n2;
            vals.push(<NTY as NumFromBytes<NTY, END>>::convert(
                &buf[i1..(i1 + es)],
                big_endian,
            ));
        }
        Ok(vals)
    }
}

pub trait EventValueShape<NTY, END>: EventValueFromBytes<NTY, END> + Send + Unpin
where
    NTY: NumFromBytes<NTY, END>,
{
    type NumXAggToSingleBin: EventsNodeProcessor<Input = <Self as EventValueFromBytes<NTY, END>>::Output>;
    type NumXAggToNBins: EventsNodeProcessor<Input = <Self as EventValueFromBytes<NTY, END>>::Output>;
    type NumXAggPlain: EventsNodeProcessor<Input = <Self as EventValueFromBytes<NTY, END>>::Output>;
}

pub struct EventValuesDim0Case<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventValuesDim0Case<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

impl<NTY, END> EventValueShape<NTY, END> for EventValuesDim0Case<NTY>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
{
    type NumXAggToSingleBin = Identity<NTY>;
    // TODO is this sufficient?
    type NumXAggToNBins = Identity<NTY>;
    type NumXAggPlain = Identity<NTY>;
}

pub struct EventValuesDim1Case<NTY> {
    n: u32,
    _m1: PhantomData<NTY>,
}

impl<NTY> EventValuesDim1Case<NTY> {
    pub fn new(n: u32) -> Self {
        Self { n, _m1: PhantomData }
    }
}

impl<NTY, END> EventValueShape<NTY, END> for EventValuesDim1Case<NTY>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
{
    type NumXAggToSingleBin = WaveXBinner<NTY>;
    type NumXAggToNBins = WaveNBinner<NTY>;
    type NumXAggPlain = WavePlainProc<NTY>;
}

// TODO add pulse.
// TODO change name, it's not only about values, but more like batch of whole events.
#[derive(Serialize, Deserialize)]
pub struct EventValues<VT> {
    pub tss: Vec<u64>,
    pub values: Vec<VT>,
}

impl<VT> EventValues<VT> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            values: vec![],
        }
    }
}

impl<VT> std::fmt::Debug for EventValues<VT>
where
    VT: std::fmt::Debug,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "count {}  ts {:?} .. {:?}  vals {:?} .. {:?}",
            self.tss.len(),
            self.tss.first(),
            self.tss.last(),
            self.values.first(),
            self.values.last(),
        )
    }
}

impl<VT> WithLen for EventValues<VT> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<VT> WithTimestamps for EventValues<VT> {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl<VT> RangeOverlapInfo for EventValues<VT> {
    fn ends_before(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts < range.beg,
            None => true,
        }
    }

    fn ends_after(&self, range: NanoRange) -> bool {
        match self.tss.last() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }

    fn starts_after(&self, range: NanoRange) -> bool {
        match self.tss.first() {
            Some(&ts) => ts >= range.end,
            None => panic!(),
        }
    }
}

impl<VT> FitsInside for EventValues<VT> {
    fn fits_inside(&self, range: NanoRange) -> Fits {
        if self.tss.is_empty() {
            Fits::Empty
        } else {
            let t1 = *self.tss.first().unwrap();
            let t2 = *self.tss.last().unwrap();
            if t2 < range.beg {
                Fits::Lower
            } else if t1 > range.end {
                Fits::Greater
            } else if t1 < range.beg && t2 > range.end {
                Fits::PartlyLowerAndGreater
            } else if t1 < range.beg {
                Fits::PartlyLower
            } else if t2 > range.end {
                Fits::PartlyGreater
            } else {
                Fits::Inside
            }
        }
    }
}

impl<VT> FilterFittingInside for EventValues<VT> {
    fn filter_fitting_inside(self, fit_range: NanoRange) -> Option<Self> {
        match self.fits_inside(fit_range) {
            Fits::Inside | Fits::PartlyGreater | Fits::PartlyLower | Fits::PartlyLowerAndGreater => Some(self),
            _ => None,
        }
    }
}

impl<NTY> PushableIndex for EventValues<NTY>
where
    NTY: NumOps,
{
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.values.push(src.values[ix]);
    }
}

impl<NTY> Appendable for EventValues<NTY>
where
    NTY: NumOps,
{
    fn empty() -> Self {
        Self::empty()
    }

    fn append(&mut self, src: &Self) {
        self.tss.extend_from_slice(&src.tss);
        self.values.extend_from_slice(&src.values);
    }
}

impl<NTY> ReadableFromFile for EventValues<NTY>
where
    NTY: NumOps,
{
    fn read_from_file(_file: File) -> Result<ReadPbv<Self>, Error> {
        // TODO refactor types such that this can be removed.
        panic!()
    }

    fn from_buf(_buf: &[u8]) -> Result<Self, Error> {
        panic!()
    }
}

impl<NTY> TimeBinnableType for EventValues<NTY>
where
    NTY: NumOps,
{
    type Output = MinMaxAvgBins<NTY>;
    type Aggregator = EventValuesAggregator<NTY>;

    fn aggregator(range: NanoRange, _bin_count: usize) -> Self::Aggregator {
        Self::Aggregator::new(range)
    }
}

pub struct EventValuesCollector<NTY> {
    vals: EventValues<NTY>,
    range_complete: bool,
    timed_out: bool,
}

impl<NTY> EventValuesCollector<NTY> {
    pub fn new() -> Self {
        Self {
            vals: EventValues::empty(),
            range_complete: false,
            timed_out: false,
        }
    }
}

impl<NTY> WithLen for EventValuesCollector<NTY> {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

#[derive(Serialize)]
pub struct EventValuesCollectorOutput<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: Vec<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: Vec<u64>,
    values: Vec<NTY>,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "finalisedRange")]
    range_complete: bool,
    #[serde(skip_serializing_if = "Bool::is_false", rename = "timedOut")]
    timed_out: bool,
}

impl<NTY> Collector for EventValuesCollector<NTY>
where
    NTY: NumOps,
{
    type Input = EventValues<NTY>;
    type Output = EventValuesCollectorOutput<NTY>;

    fn ingest(&mut self, src: &Self::Input) {
        self.vals.append(src);
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(self) -> Result<Self::Output, Error> {
        let tst = ts_offs_from_abs(&self.vals.tss);
        let ret = Self::Output {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            values: self.vals.values,
            range_complete: self.range_complete,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<NTY> Collectable for EventValues<NTY>
where
    NTY: NumOps,
{
    type Collector = EventValuesCollector<NTY>;

    fn new_collector(_bin_count_exp: u32) -> Self::Collector {
        Self::Collector::new()
    }
}

pub struct EventsDecodedStream<NTY, END, EVS>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
    END: Endianness,
    EVS: EventValueShape<NTY, END>,
{
    evs: EVS,
    event_blobs: EventBlobsComplete,
    completed: bool,
    errored: bool,
    _m1: PhantomData<NTY>,
    _m2: PhantomData<END>,
    _m3: PhantomData<EVS>,
}

impl<NTY, END, EVS> EventsDecodedStream<NTY, END, EVS>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
    END: Endianness,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END>,
{
    pub fn new(evs: EVS, event_blobs: EventBlobsComplete) -> Self {
        Self {
            evs,
            event_blobs,
            completed: false,
            errored: false,
            _m1: PhantomData,
            _m2: PhantomData,
            _m3: PhantomData,
        }
    }

    fn decode(&mut self, ev: &EventFull) -> Result<EventValues<<EVS as EventValueFromBytes<NTY, END>>::Output>, Error> {
        let mut ret = EventValues::empty();
        for i1 in 0..ev.tss.len() {
            // TODO check that dtype, event endianness and event shape match our static
            // expectation about the data in this channel.
            let _ty = &ev.scalar_types[i1];
            let be = ev.be[i1];
            // Too bad, data on disk is inconsistent, can not rely on endian as stated in channel config.
            if false && be != END::is_big() {
                return Err(Error::with_msg(format!(
                    "endian mismatch in event  got {}  exp {}",
                    be,
                    END::is_big()
                )));
            }
            let decomp = ev.decomps[i1].as_ref().unwrap().as_ref();
            let val = self.evs.convert(decomp, be)?;
            ret.tss.push(ev.tss[i1]);
            ret.values.push(val);
        }
        Ok(ret)
    }
}

impl<NTY, END, EVS> Stream for EventsDecodedStream<NTY, END, EVS>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
    END: Endianness,
    EVS: EventValueShape<NTY, END> + EventValueFromBytes<NTY, END>,
{
    type Item =
        Result<StreamItem<RangeCompletableItem<EventValues<<EVS as EventValueFromBytes<NTY, END>>::Output>>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("poll_next on completed")
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else {
                match self.event_blobs.poll_next_unpin(cx) {
                    Ready(item) => match item {
                        Some(item) => match item {
                            Ok(item) => match item {
                                StreamItem::DataItem(item) => match item {
                                    RangeCompletableItem::RangeComplete => {
                                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                                    }
                                    RangeCompletableItem::Data(item) => match self.decode(&item) {
                                        Ok(res) => {
                                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(res)))))
                                        }
                                        Err(e) => {
                                            self.errored = true;
                                            Ready(Some(Err(e)))
                                        }
                                    },
                                },
                                StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                                StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                            },
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e)))
                            }
                        },
                        None => {
                            self.completed = true;
                            Ready(None)
                        }
                    },
                    Pending => Pending,
                }
            };
        }
    }
}
