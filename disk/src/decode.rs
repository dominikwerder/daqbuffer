use crate::agg::enp::Identity;
use crate::eventblobs::EventChunkerMultifile;
use crate::eventchunker::EventFull;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use items::eventsitem::EventsItem;
use items::scalarevents::ScalarEvents;
use items::numops::{BoolNum, NumOps};
use items::plainevents::{PlainEvents, ScalarPlainEvents};
use items::waveevents::{WaveEvents, WaveNBinner, WavePlainProc, WaveXBinner};
use items::{Appendable, EventAppendable, EventsNodeProcessor, RangeCompletableItem, Sitemty, StreamItem};
use netpod::{ScalarType, Shape};
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
    type Batch = ScalarEvents<NTY>;

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

pub struct EventsItemStream {
    inp: Pin<Box<dyn Stream<Item = Sitemty<EventFull>>>>,
    done: bool,
    complete: bool,
}

impl EventsItemStream {
    pub fn new(inp: Pin<Box<dyn Stream<Item = Sitemty<EventFull>>>>) -> Self {
        Self {
            inp,
            done: false,
            complete: false,
        }
    }

    // TODO need some default expectation about the content type, because real world data does not
    // always contain that information per event, or even contains wrong information.
    fn decode(&mut self, ev: &EventFull) -> Result<Option<EventsItem>, Error> {
        // TODO define expected endian from parameters:
        let big_endian = false;
        // TODO:
        let mut tyi = None;
        let mut ret = None;
        for i1 in 0..ev.tss.len() {
            let ts = ev.tss[i1];
            let pulse = ev.pulses[i1];
            // TODO check that dtype, event endianness and event shape match our static
            // expectation about the data in this channel.
            let _ty = &ev.scalar_types[i1];
            let be = ev.be[i1];
            if be != big_endian {
                return Err(Error::with_msg(format!("big endian mismatch {} vs {}", be, big_endian)));
            }
            // TODO bad, data on disk is inconsistent, can not rely on endian as stated in channel config.
            let decomp = ev.decomp(i1);
            // If not done yet, infer the actual type from the (undocumented) combinations of channel
            // config parameters and values in the event data.
            // TODO
            match &tyi {
                Some(_) => {}
                None => {
                    //let cont = EventValues::<f64>::empty();
                    tyi = Some((ev.scalar_types[i1].clone(), ev.shapes[i1].clone()));
                    match &tyi.as_ref().unwrap().1 {
                        Shape::Scalar => match &tyi.as_ref().unwrap().0 {
                            ScalarType::U8 => {
                                // TODO
                                let cont = ScalarEvents::<i8>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I8(cont))));
                            }
                            ScalarType::U16 => {
                                // TODO
                                let cont = ScalarEvents::<i16>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I16(cont))));
                            }
                            ScalarType::U32 => {
                                // TODO
                                let cont = ScalarEvents::<i32>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I32(cont))));
                            }
                            ScalarType::U64 => {
                                // TODO
                                let cont = ScalarEvents::<i32>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I32(cont))));
                            }
                            ScalarType::I8 => {
                                let cont = ScalarEvents::<i8>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I8(cont))));
                            }
                            ScalarType::I16 => {
                                let cont = ScalarEvents::<i16>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I16(cont))));
                            }
                            ScalarType::I32 => {
                                let cont = ScalarEvents::<i32>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I32(cont))));
                            }
                            ScalarType::I64 => {
                                // TODO
                                let cont = ScalarEvents::<i32>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I32(cont))));
                            }
                            ScalarType::F32 => {
                                let cont = ScalarEvents::<f32>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::F32(cont))));
                            }
                            ScalarType::F64 => {
                                let cont = ScalarEvents::<f64>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::F64(cont))));
                            }
                            ScalarType::BOOL => {
                                // TODO
                                let cont = ScalarEvents::<i8>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::I8(cont))));
                            }
                        },
                        Shape::Wave(_) => todo!(),
                        Shape::Image(..) => todo!(),
                    }
                }
            };
            // TODO here, I expect that we found the type.
            let tyi = tyi.as_ref().unwrap();
            match &tyi.1 {
                Shape::Scalar => match &tyi.0 {
                    ScalarType::U8 => todo!(),
                    ScalarType::U16 => todo!(),
                    ScalarType::U32 => todo!(),
                    ScalarType::U64 => todo!(),
                    ScalarType::I8 => todo!(),
                    ScalarType::I16 => todo!(),
                    ScalarType::I32 => todo!(),
                    ScalarType::I64 => todo!(),
                    ScalarType::F32 => todo!(),
                    ScalarType::F64 => {
                        let conv = EventValuesDim0Case::<f64>::new();
                        let val = EventValueFromBytes::<_, LittleEndian>::convert(&conv, decomp, big_endian)?;
                        match &mut ret {
                            Some(ret) => match ret {
                                EventsItem::Plain(ret) => match ret {
                                    PlainEvents::Scalar(ret) => match ret {
                                        ScalarPlainEvents::F64(ret) => {
                                            ret.tss.push(ts);
                                            // TODO
                                            let _ = pulse;
                                            ret.values.push(val);
                                        }
                                        _ => panic!(),
                                    },
                                    PlainEvents::Wave(_) => panic!(),
                                },
                                EventsItem::XBinnedEvents(_) => todo!(),
                            },
                            None => panic!(),
                        }
                    }
                    ScalarType::BOOL => todo!(),
                },
                Shape::Wave(_) => todo!(),
                Shape::Image(_, _) => todo!(),
            }
            //let val = self.evs.convert(decomp, be)?;
            //let k = <<EVS as EventValueFromBytes<NTY, END>>::Batch as EventAppendable>::append_event(ret, ev.tss[i1], val);
        }
        Ok(ret)
    }
}

impl Stream for EventsItemStream {
    type Item = Sitemty<EventsItem>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                match self.inp.poll_next_unpin(cx) {
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
                                            self.done = true;
                                            Ready(Some(Err(e)))
                                        }
                                    },
                                },
                                StreamItem::Log(item) => Ready(Some(Ok(StreamItem::Log(item)))),
                                StreamItem::Stats(item) => Ready(Some(Ok(StreamItem::Stats(item)))),
                            },
                            Err(e) => {
                                self.done = true;
                                Ready(Some(Err(e)))
                            }
                        },
                        None => {
                            self.done = true;
                            Ready(None)
                        }
                    },
                    Pending => Pending,
                }
            };
        }
    }
}
