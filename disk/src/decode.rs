use crate::agg::enp::Identity;
use crate::eventblobs::EventChunkerMultifile;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items::eventfull::EventFull;
use items::eventsitem::EventsItem;
use items::numops::BoolNum;
use items::numops::NumOps;
use items::numops::StringNum;
use items::plainevents::PlainEvents;
use items::plainevents::ScalarPlainEvents;
use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use items::waveevents::WaveNBinner;
use items::waveevents::WavePlainProc;
use items::waveevents::WaveXBinner;
use items::Appendable;
use items::EventAppendable;
use items::EventsNodeProcessor;
use items::RangeCompletableItem;
use items::Sitemty;
use items::StreamItem;
use items_0::scalar_ops::ScalarOps;
use items_0::Events;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
#[allow(unused)]
use netpod::log::*;
use netpod::AggKind;
use netpod::ScalarType;
use netpod::Shape;
use std::marker::PhantomData;
use std::mem;
use std::mem::size_of;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

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

pub enum Endian {
    Little,
    Big,
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

impl NumFromBytes<StringNum, LittleEndian> for StringNum {
    fn convert(buf: &[u8], _big_endian: bool) -> StringNum {
        if false {
            // TODO remove
            netpod::log::error!("TODO NumFromBytes for StringNum  buf len {}", buf.len());
        }
        let s = if buf.len() >= 250 {
            String::from_utf8_lossy(&buf[..250])
        } else {
            String::from_utf8_lossy(buf)
        };
        Self(s.into())
    }
}

impl NumFromBytes<StringNum, BigEndian> for StringNum {
    fn convert(buf: &[u8], _big_endian: bool) -> StringNum {
        if false {
            // TODO remove
            netpod::log::error!("TODO NumFromBytes for StringNum  buf len {}", buf.len());
        }
        let s = if buf.len() >= 250 {
            String::from_utf8_lossy(&buf[..250])
        } else {
            String::from_utf8_lossy(buf)
        };
        Self(s.into())
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

pub trait ScalarValueFromBytes<STY> {
    fn convert(buf: &[u8], endian: Endian) -> Result<STY, Error>;
    fn convert_dim1(buf: &[u8], endian: Endian, n: usize) -> Result<Vec<STY>, Error>;
}

macro_rules! impl_scalar_value_from_bytes {
    ($nty:ident, $nl:expr) => {
        impl ScalarValueFromBytes<$nty> for $nty {
            // Error in data on disk:
            // Can not rely on byte order as stated in the channel config.
            // Endianness in sf-databuffer can be specified for each event.
            fn convert(buf: &[u8], endian: Endian) -> Result<$nty, Error> {
                //$nty::$ec(*arrayref::array_ref![buf, 0, $nl])
                use Endian::*;
                let ret = match endian {
                    Little => $nty::from_le_bytes(buf[..$nl].try_into()?),
                    Big => $nty::from_be_bytes(buf[..$nl].try_into()?),
                };
                Ok(ret)
            }

            fn convert_dim1(buf: &[u8], endian: Endian, n: usize) -> Result<Vec<$nty>, Error> {
                let ret = buf
                    .chunks_exact(n.min($nl))
                    .map(|b2| {
                        use Endian::*;
                        let ret = match endian {
                            Little => $nty::from_le_bytes(b2[..$nl].try_into().unwrap()),
                            Big => $nty::from_be_bytes(b2[..$nl].try_into().unwrap()),
                        };
                        ret
                    })
                    .collect();
                Ok(ret)
            }
        }
    };
}

impl_scalar_value_from_bytes!(u8, 1);
impl_scalar_value_from_bytes!(u16, 2);
impl_scalar_value_from_bytes!(u32, 4);
impl_scalar_value_from_bytes!(u64, 8);
impl_scalar_value_from_bytes!(i8, 1);
impl_scalar_value_from_bytes!(i16, 2);
impl_scalar_value_from_bytes!(i32, 4);
impl_scalar_value_from_bytes!(i64, 8);
impl_scalar_value_from_bytes!(f32, 4);
impl_scalar_value_from_bytes!(f64, 8);

impl ScalarValueFromBytes<String> for String {
    fn convert(buf: &[u8], _endian: Endian) -> Result<String, Error> {
        let s = if buf.len() >= 255 {
            String::from_utf8_lossy(&buf[..255])
        } else {
            String::from_utf8_lossy(buf)
        };
        Ok(s.into())
    }

    fn convert_dim1(buf: &[u8], _endian: Endian, _n: usize) -> Result<Vec<String>, Error> {
        let s = if buf.len() >= 255 {
            String::from_utf8_lossy(&buf[..255])
        } else {
            String::from_utf8_lossy(buf)
        };
        Ok(vec![s.into()])
    }
}

impl ScalarValueFromBytes<bool> for bool {
    fn convert(buf: &[u8], _endian: Endian) -> Result<bool, Error> {
        if buf.len() >= 1 {
            if buf[0] != 0 {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    fn convert_dim1(buf: &[u8], _endian: Endian, n: usize) -> Result<Vec<bool>, Error> {
        let nn = buf.len().min(n);
        Ok(buf.iter().take(nn).map(|&x| x != 0).collect())
    }
}

pub trait ValueFromBytes: Send {
    fn convert(&self, ts: u64, pulse: u64, buf: &[u8], endian: Endian, events: &mut dyn Events) -> Result<(), Error>;
}

pub trait ValueDim0FromBytes {
    fn convert(&self, ts: u64, pulse: u64, buf: &[u8], endian: Endian, events: &mut dyn Events) -> Result<(), Error>;
}

pub trait ValueDim1FromBytes {
    fn convert(&self, ts: u64, pulse: u64, buf: &[u8], endian: Endian, events: &mut dyn Events) -> Result<(), Error>;
}

pub struct ValueDim0FromBytesImpl<STY>
where
    STY: ScalarOps,
{
    _m1: PhantomData<STY>,
}

impl<STY> ValueDim0FromBytesImpl<STY>
where
    STY: ScalarOps + ScalarValueFromBytes<STY>,
{
    fn boxed() -> Box<dyn ValueFromBytes> {
        Box::new(Self {
            _m1: Default::default(),
        })
    }
}

impl<STY> ValueDim0FromBytes for ValueDim0FromBytesImpl<STY>
where
    STY: ScalarOps + ScalarValueFromBytes<STY>,
{
    fn convert(&self, ts: u64, pulse: u64, buf: &[u8], endian: Endian, events: &mut dyn Events) -> Result<(), Error> {
        if let Some(evs) = events.as_any_mut().downcast_mut::<EventsDim0<STY>>() {
            let v = <STY as ScalarValueFromBytes<STY>>::convert(buf, endian)?;
            evs.values.push_back(v);
            evs.tss.push_back(ts);
            evs.pulses.push_back(pulse);
            Ok(())
        } else {
            Err(Error::with_msg_no_trace("unexpected container"))
        }
    }
}

impl<STY> ValueFromBytes for ValueDim0FromBytesImpl<STY>
where
    STY: ScalarOps + ScalarValueFromBytes<STY>,
{
    fn convert(&self, ts: u64, pulse: u64, buf: &[u8], endian: Endian, events: &mut dyn Events) -> Result<(), Error> {
        ValueDim0FromBytes::convert(self, ts, pulse, buf, endian, events)
    }
}

pub struct ValueDim1FromBytesImpl<STY>
where
    STY: ScalarOps,
{
    shape: Shape,
    _m1: PhantomData<STY>,
}

impl<STY> ValueDim1FromBytesImpl<STY>
where
    STY: ScalarOps + ScalarValueFromBytes<STY>,
{
    fn boxed(shape: Shape) -> Box<dyn ValueFromBytes> {
        Box::new(Self {
            shape,
            _m1: Default::default(),
        })
    }
}

impl<STY> ValueFromBytes for ValueDim1FromBytesImpl<STY>
where
    STY: ScalarOps + ScalarValueFromBytes<STY>,
{
    fn convert(&self, ts: u64, pulse: u64, buf: &[u8], endian: Endian, events: &mut dyn Events) -> Result<(), Error> {
        ValueDim1FromBytes::convert(self, ts, pulse, buf, endian, events)
    }
}

impl<STY> ValueDim1FromBytes for ValueDim1FromBytesImpl<STY>
where
    STY: ScalarOps + ScalarValueFromBytes<STY>,
{
    fn convert(&self, ts: u64, pulse: u64, buf: &[u8], endian: Endian, events: &mut dyn Events) -> Result<(), Error> {
        if let Some(evs) = events.as_any_mut().downcast_mut::<EventsDim1<STY>>() {
            let n = if let Shape::Wave(n) = self.shape {
                n
            } else {
                return Err(Error::with_msg_no_trace("ValueDim1FromBytesImpl bad shape"));
            };
            let v = <STY as ScalarValueFromBytes<STY>>::convert_dim1(buf, endian, n as _)?;
            evs.values.push_back(v);
            evs.tss.push_back(ts);
            evs.pulses.push_back(pulse);
            Ok(())
        } else {
            Err(Error::with_msg_no_trace("unexpected container"))
        }
    }
}

fn make_scalar_conv(
    scalar_type: &ScalarType,
    shape: &Shape,
    agg_kind: &AggKind,
) -> Result<Box<dyn ValueFromBytes>, Error> {
    let ret = match agg_kind {
        AggKind::EventBlobs => todo!("make_scalar_conv  EventBlobs"),
        AggKind::Plain | AggKind::DimXBinsN(_) | AggKind::DimXBins1 | AggKind::TimeWeightedScalar => match shape {
            Shape::Scalar => match scalar_type {
                ScalarType::U8 => ValueDim0FromBytesImpl::<u8>::boxed(),
                ScalarType::U16 => ValueDim0FromBytesImpl::<u16>::boxed(),
                ScalarType::U32 => ValueDim0FromBytesImpl::<u32>::boxed(),
                ScalarType::U64 => ValueDim0FromBytesImpl::<u64>::boxed(),
                ScalarType::I8 => ValueDim0FromBytesImpl::<i8>::boxed(),
                ScalarType::I16 => ValueDim0FromBytesImpl::<i16>::boxed(),
                ScalarType::I32 => ValueDim0FromBytesImpl::<i32>::boxed(),
                ScalarType::I64 => ValueDim0FromBytesImpl::<i64>::boxed(),
                ScalarType::F32 => ValueDim0FromBytesImpl::<f32>::boxed(),
                ScalarType::F64 => ValueDim0FromBytesImpl::<f64>::boxed(),
                ScalarType::BOOL => ValueDim0FromBytesImpl::<bool>::boxed(),
                ScalarType::STRING => ValueDim0FromBytesImpl::<String>::boxed(),
            },
            Shape::Wave(_) => {
                let shape = shape.clone();
                match scalar_type {
                    ScalarType::U8 => ValueDim1FromBytesImpl::<u8>::boxed(shape),
                    ScalarType::U16 => ValueDim1FromBytesImpl::<u16>::boxed(shape),
                    ScalarType::U32 => ValueDim1FromBytesImpl::<u32>::boxed(shape),
                    ScalarType::U64 => ValueDim1FromBytesImpl::<u64>::boxed(shape),
                    ScalarType::I8 => ValueDim1FromBytesImpl::<i8>::boxed(shape),
                    ScalarType::I16 => ValueDim1FromBytesImpl::<i16>::boxed(shape),
                    ScalarType::I32 => ValueDim1FromBytesImpl::<i32>::boxed(shape),
                    ScalarType::I64 => ValueDim1FromBytesImpl::<i64>::boxed(shape),
                    ScalarType::F32 => ValueDim1FromBytesImpl::<f32>::boxed(shape),
                    ScalarType::F64 => ValueDim1FromBytesImpl::<f64>::boxed(shape),
                    ScalarType::BOOL => ValueDim1FromBytesImpl::<bool>::boxed(shape),
                    ScalarType::STRING => ValueDim1FromBytesImpl::<String>::boxed(shape),
                }
            }
            Shape::Image(_, _) => todo!("make_scalar_conv  Image"),
        },
    };
    Ok(ret)
}

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
            let k = <<EVS as EventValueFromBytes<NTY, END>>::Batch as EventAppendable>::append_event(
                ret,
                ev.tss[i1],
                ev.pulses[i1],
                val,
            );
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

pub struct EventsDynStream {
    scalar_type: ScalarType,
    shape: Shape,
    agg_kind: AggKind,
    events_full: EventChunkerMultifile,
    events_out: Box<dyn Events>,
    scalar_conv: Box<dyn ValueFromBytes>,
    emit_threshold: usize,
    done: bool,
    complete: bool,
}

impl EventsDynStream {
    pub fn new(
        scalar_type: ScalarType,
        shape: Shape,
        agg_kind: AggKind,
        events_full: EventChunkerMultifile,
    ) -> Result<Self, Error> {
        let st = &scalar_type;
        let sh = &shape;
        let ag = &agg_kind;
        let events_out = items_2::empty_events_dyn_ev(st, sh, ag)?;
        let scalar_conv = make_scalar_conv(st, sh, ag)?;
        let emit_threshold = match &shape {
            Shape::Scalar => 2048,
            Shape::Wave(_) => 64,
            Shape::Image(_, _) => 1,
        };
        let ret = Self {
            scalar_type,
            shape,
            agg_kind,
            events_full,
            events_out,
            scalar_conv,
            emit_threshold,
            done: false,
            complete: false,
        };
        Ok(ret)
    }

    fn replace_events_out(&mut self) -> Result<Box<dyn Events>, Error> {
        let st = &self.scalar_type;
        let sh = &self.shape;
        let ag = &self.agg_kind;
        let empty = items_2::empty_events_dyn_ev(st, sh, ag)?;
        let evs = mem::replace(&mut self.events_out, empty);
        Ok(evs)
    }

    fn handle_event_full(&mut self, item: EventFull) -> Result<(), Error> {
        use items::WithLen;
        if item.len() >= self.emit_threshold {
            info!("handle_event_full  item len {}", item.len());
        }
        for (((buf, &be), &ts), &pulse) in item
            .blobs
            .iter()
            .zip(item.be.iter())
            .zip(item.tss.iter())
            .zip(item.pulses.iter())
        {
            let endian = if be { Endian::Big } else { Endian::Little };
            self.scalar_conv
                .convert(ts, pulse, buf, endian, self.events_out.as_mut())?;
        }
        Ok(())
    }

    fn handle_stream_item(
        &mut self,
        item: StreamItem<RangeCompletableItem<EventFull>>,
    ) -> Result<Option<Sitemty<Box<dyn items_0::Events>>>, Error> {
        let ret = match item {
            StreamItem::DataItem(item) => match item {
                RangeCompletableItem::RangeComplete => {
                    Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)))
                }
                RangeCompletableItem::Data(item) => match self.handle_event_full(item) {
                    Ok(()) => {
                        // TODO collect stats.
                        if self.events_out.len() >= self.emit_threshold {
                            let evs = self.replace_events_out()?;
                            Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(evs))))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(e)),
                },
            },
            StreamItem::Log(item) => Some(Ok(StreamItem::Log(item))),
            StreamItem::Stats(item) => Some(Ok(StreamItem::Stats(item))),
        };
        Ok(ret)
    }
}

impl Stream for EventsDynStream {
    type Item = Sitemty<Box<dyn items_0::Events>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                match self.events_full.poll_next_unpin(cx) {
                    Ready(Some(Ok(item))) => match self.handle_stream_item(item) {
                        Ok(Some(item)) => Ready(Some(item)),
                        Ok(None) => continue,
                        Err(e) => {
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        // Produce a last one even if it is empty.
                        match self.replace_events_out() {
                            Ok(item) => {
                                self.done = true;
                                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
                            }
                            Err(e) => {
                                self.done = true;
                                Ready(Some(Err(e)))
                            }
                        }
                    }
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
                            ScalarType::STRING => {
                                // TODO
                                let cont = ScalarEvents::<String>::empty();
                                ret = Some(EventsItem::Plain(PlainEvents::Scalar(ScalarPlainEvents::String(cont))));
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
                                },
                                EventsItem::XBinnedEvents(_) => todo!(),
                            },
                            None => panic!(),
                        }
                    }
                    ScalarType::BOOL => todo!(),
                    ScalarType::STRING => todo!(),
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
