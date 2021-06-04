use crate::agg::eventbatch::MinMaxAvgScalarEventBatch;
use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatch;
use crate::agg::streams::StreamItem;
use crate::binned::{EventsNodeProcessor, NumOps, RangeCompletableItem};
use crate::eventblobs::EventBlobsComplete;
use crate::eventchunker::EventFull;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use netpod::ScalarType;
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

impl NumFromBytes<i32, LittleEndian> for i32 {
    fn convert(buf: &[u8]) -> i32 {
        i32::from_le_bytes(*arrayref::array_ref![buf, 0, 4])
    }
}

impl NumFromBytes<i32, BigEndian> for i32 {
    fn convert(buf: &[u8]) -> i32 {
        i32::from_be_bytes(*arrayref::array_ref![buf, 0, 4])
    }
}

pub trait EventValueFromBytes<NTY, END>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output;
    fn convert(buf: &[u8]) -> Self::Output;
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim0Case<NTY>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output = NTY;
    fn convert(buf: &[u8]) -> Self::Output {
        NTY::convert(buf)
    }
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim1Case<NTY>
where
    NTY: NumFromBytes<NTY, END>,
{
    type Output = Vec<NTY>;
    fn convert(buf: &[u8]) -> Self::Output {
        let es = size_of::<NTY>();
        let n1 = buf.len() / es;
        let mut vals = vec![];
        // TODO could optimize using unsafe code..
        for n2 in 0..n1 {
            let i1 = es * n2;
            vals.push(<NTY as NumFromBytes<NTY, END>>::convert(&buf[i1..(i1 + es)]));
        }
        vals
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
    type Output = MinMaxAvgScalarEventBatch;
    fn process(inp: &EventValues<Self::Input>) -> Self::Output {
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

impl<NTY> EventsNodeProcessor for ProcBB<NTY> {
    type Input = Vec<NTY>;
    type Output = MinMaxAvgScalarBinBatch;
    fn process(inp: &EventValues<Self::Input>) -> Self::Output {
        todo!()
    }
}

impl<NTY, END> EventValueShape<NTY, END> for EventValuesDim1Case<NTY>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
{
    type NumXAggToSingleBin = ProcBB<NTY>;
    type NumXAggToNBins = ProcBB<NTY>;
}

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
    pub fn new(event_blobs: EventBlobsComplete) -> Self {
        Self {
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

            let val = <EVS as EventValueFromBytes<NTY, END>>::convert(decomp);
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
