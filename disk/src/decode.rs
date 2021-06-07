use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::StreamItem;
use crate::binned::{EventsNodeProcessor, NumOps, RangeCompletableItem};
use crate::eventblobs::EventBlobsComplete;
use crate::eventchunker::EventFull;
use crate::frame::makeframe::{make_frame, Framable};
use bytes::BytesMut;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::mem::size_of;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait Endianness: Send + Unpin {}
pub struct LittleEndian {}
pub struct BigEndian {}
impl Endianness for LittleEndian {}
impl Endianness for BigEndian {}

pub trait NumFromBytes<NTY, END> {
    fn convert(buf: &[u8]) -> NTY;
}

macro_rules! impl_num_from_bytes_end {
    ($nty:ident, $nl:expr, $end:ident, $ec:ident) => {
        impl NumFromBytes<$nty, $end> for $nty {
            fn convert(buf: &[u8]) -> $nty {
                $nty::$ec(*arrayref::array_ref![buf, 0, $nl])
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
    fn convert(&self, buf: &[u8]) -> Result<Self::Output, Error>;
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim0Case<NTY>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output = NTY;

    fn convert(&self, buf: &[u8]) -> Result<Self::Output, Error> {
        Ok(NTY::convert(buf))
    }
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim1Case<NTY>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output = Vec<NTY>;

    fn convert(&self, buf: &[u8]) -> Result<Self::Output, Error> {
        let es = size_of::<NTY>();
        let n1 = buf.len() / es;
        if n1 != self.n as usize {
            return Err(Error::with_msg(format!("ele count  got {}  exp {}", n1, self.n)));
        }
        let mut vals = vec![];
        // TODO could optimize using unsafe code..
        for n2 in 0..n1 {
            let i1 = es * n2;
            vals.push(<NTY as NumFromBytes<NTY, END>>::convert(&buf[i1..(i1 + es)]));
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
}

pub struct EventValuesDim0Case<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventValuesDim0Case<NTY> {
    pub fn new() -> Self {
        Self { _m1: PhantomData }
    }
}

pub struct ProcAA<NTY> {
    _m1: PhantomData<NTY>,
}

impl<NTY> EventsNodeProcessor for ProcAA<NTY> {
    type Input = NTY;
    type Output = MinMaxAvgScalarBinBatch;

    fn process(_inp: EventValues<Self::Input>) -> Self::Output {
        todo!()
    }
}

impl<NTY, END> EventValueShape<NTY, END> for EventValuesDim0Case<NTY>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
{
    type NumXAggToSingleBin = ProcAA<NTY>;
    type NumXAggToNBins = ProcAA<NTY>;
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

pub struct ProcBB<NTY> {
    _m1: PhantomData<NTY>,
}

#[derive(Serialize, Deserialize)]
pub struct MinMaxAvgScalarEventBatchGen<NTY> {
    pub tss: Vec<u64>,
    pub mins: Vec<Option<NTY>>,
    pub maxs: Vec<Option<NTY>>,
    pub avgs: Vec<Option<f32>>,
}

impl<NTY> MinMaxAvgScalarEventBatchGen<NTY> {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            mins: vec![],
            maxs: vec![],
            avgs: vec![],
        }
    }
}

impl<NTY> Framable for Result<StreamItem<RangeCompletableItem<MinMaxAvgScalarEventBatchGen<NTY>>>, Error>
where
    NTY: NumOps + Serialize,
{
    fn make_frame(&self) -> Result<BytesMut, Error> {
        make_frame(self)
    }
}

impl<NTY> EventsNodeProcessor for ProcBB<NTY>
where
    NTY: NumOps,
{
    type Input = Vec<NTY>;
    type Output = MinMaxAvgScalarEventBatchGen<NTY>;

    fn process(inp: EventValues<Self::Input>) -> Self::Output {
        let nev = inp.tss.len();
        let mut ret = MinMaxAvgScalarEventBatchGen {
            tss: inp.tss,
            mins: Vec::with_capacity(nev),
            maxs: Vec::with_capacity(nev),
            avgs: Vec::with_capacity(nev),
        };
        for i1 in 0..nev {
            let mut min = None;
            let mut max = None;
            let mut sum = 0f32;
            let mut count = 0;
            let vals = &inp.values[i1];
            for i2 in 0..vals.len() {
                let v = vals[i2];
                min = match min {
                    None => Some(v),
                    Some(min) => {
                        if v < min {
                            Some(v)
                        } else {
                            Some(min)
                        }
                    }
                };
                max = match max {
                    None => Some(v),
                    Some(max) => {
                        if v > max {
                            Some(v)
                        } else {
                            Some(max)
                        }
                    }
                };
                let vf = v.as_();
                if vf.is_nan() {
                } else {
                    sum += vf;
                    count += 1;
                }
            }
            ret.mins.push(min);
            ret.maxs.push(max);
            if count == 0 {
                ret.avgs.push(None);
            } else {
                ret.avgs.push(Some(sum / count as f32));
            }
        }
        ret
    }
}

impl<NTY, END> EventValueShape<NTY, END> for EventValuesDim1Case<NTY>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
{
    type NumXAggToSingleBin = ProcBB<NTY>;
    type NumXAggToNBins = ProcBB<NTY>;
}

// TODO why not Serialize?
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
            let _be = ev.be[i1];

            let decomp = ev.decomps[i1].as_ref().unwrap().as_ref();

            let val = self.evs.convert(decomp)?;
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
