use crate::agg::enp::Identity;
use crate::eventblobs::EventChunkerMultifile;
use crate::eventchunker::EventFull;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::eventvalues::EventValues;
use items::numops::{BoolNum, NumOps};
use items::waveevents::{WaveEvents, WaveNBinner, WavePlainProc, WaveXBinner};
use items::{Appendable, EventAppendable, EventsNodeProcessor, RangeCompletableItem, StreamItem};
use std::marker::PhantomData;
use std::mem::size_of;
use std::pin::Pin;
use std::task::{Context, Poll};

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

impl NumFromBytes<BoolNum, LittleEndian> for BoolNum {
    fn convert(buf: &[u8], _big_endian: bool) -> BoolNum {
        BoolNum(buf[0])
    }
}

impl NumFromBytes<BoolNum, BigEndian> for BoolNum {
    fn convert(buf: &[u8], _big_endian: bool) -> BoolNum {
        BoolNum(buf[0])
    }
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
    type Batch: Appendable + EventAppendable<Value = Self::Output>;
    // The written data on disk has errors:
    // The endian as stated in the channel config does not match written events.
    // Therefore, can not rely on that but have to check for each single event...
    fn convert(&self, buf: &[u8], big_endian: bool) -> Result<Self::Output, Error>;
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim0Case<NTY>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
{
    type Output = NTY;
    type Batch = EventValues<NTY>;

    fn convert(&self, buf: &[u8], big_endian: bool) -> Result<Self::Output, Error> {
        Ok(NTY::convert(buf, big_endian))
    }
}

impl<NTY, END> EventValueFromBytes<NTY, END> for EventValuesDim1Case<NTY>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
{
    type Output = Vec<NTY>;
    type Batch = WaveEvents<NTY>;

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
    type NumXAggToSingleBin: EventsNodeProcessor<Input = <Self as EventValueFromBytes<NTY, END>>::Batch>;
    type NumXAggToNBins: EventsNodeProcessor<Input = <Self as EventValueFromBytes<NTY, END>>::Batch>;
    type NumXAggPlain: EventsNodeProcessor<Input = <Self as EventValueFromBytes<NTY, END>>::Batch>;
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

pub struct EventsDecodedStream<NTY, END, EVS>
where
    NTY: NumOps + NumFromBytes<NTY, END>,
    END: Endianness,
    EVS: EventValueShape<NTY, END>,
{
    evs: EVS,
    event_blobs: EventChunkerMultifile,
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
    pub fn new(evs: EVS, event_blobs: EventChunkerMultifile) -> Self {
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

    fn decode(&mut self, ev: &EventFull) -> Result<Option<<EVS as EventValueFromBytes<NTY, END>>::Batch>, Error> {
        //let mut ret = <<EVS as EventValueFromBytes<NTY, END>>::Batch as Appendable>::empty();
        //let mut ret = EventValues::<<EVS as EventValueFromBytes<NTY, END>>::Output>::empty();
        let mut ret = None;
        //ret.tss.reserve(ev.tss.len());
        //ret.values.reserve(ev.tss.len());
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
            let k =
                <<EVS as EventValueFromBytes<NTY, END>>::Batch as EventAppendable>::append_event(ret, ev.tss[i1], val);
            ret = Some(k);
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
    type Item = Result<StreamItem<RangeCompletableItem<<EVS as EventValueFromBytes<NTY, END>>::Batch>>, Error>;

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
                                        Ok(res) => match res {
                                            Some(res) => {
                                                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(res)))))
                                            }
                                            None => {
                                                continue;
                                            }
                                        },
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
