#[cfg(feature = "devread")]
pub mod generated;
#[cfg(not(feature = "devread"))]
pub mod generated {}
pub mod archeng;
#[cfg(feature = "devread")]
pub mod parse;
#[cfg(not(feature = "devread"))]
pub mod parsestub;

use err::Error;
use items::eventvalues::EventValues;
use items::numops::NumOps;
use items::waveevents::{WaveEvents, WaveXBinner};
use items::xbinnedscalarevents::XBinnedScalarEvents;
use items::xbinnedwaveevents::XBinnedWaveEvents;
use items::{Appendable, Clearable, EventsNodeProcessor, PushableIndex, SitemtyFrameType, WithLen, WithTimestamps};
use netpod::{AggKind, ByteOrder, HasScalarType, HasShape, ScalarType, Shape};
#[cfg(not(feature = "devread"))]
pub use parsestub as parse;

pub mod events;
#[cfg(feature = "devread")]
#[cfg(test)]
pub mod test;

fn unescape_archapp_msg(inp: &[u8], mut ret: Vec<u8>) -> Result<Vec<u8>, Error> {
    ret.clear();
    let mut esc = false;
    for &k in inp.iter() {
        if k == 0x1b {
            esc = true;
        } else if esc {
            if k == 0x1 {
                ret.push(0x1b);
            } else if k == 0x2 {
                ret.push(0xa);
            } else if k == 0x3 {
                ret.push(0xd);
            } else {
                return Err(Error::with_msg_no_trace("malformed escaped archapp message"));
            }
            esc = false;
        } else {
            ret.push(k);
        }
    }
    Ok(ret)
}

#[derive(Debug)]
pub enum ScalarPlainEvents {
    Byte(EventValues<i8>),
    Short(EventValues<i16>),
    Int(EventValues<i32>),
    Float(EventValues<f32>),
    Double(EventValues<f64>),
}

impl ScalarPlainEvents {
    pub fn variant_name(&self) -> String {
        use ScalarPlainEvents::*;
        match self {
            Byte(_) => format!("Byte"),
            Short(_) => format!("Short"),
            Int(_) => format!("Int"),
            Float(_) => format!("Float"),
            Double(_) => format!("Double"),
        }
    }
}

impl Clearable for ScalarPlainEvents {
    fn clear(&mut self) {
        match self {
            ScalarPlainEvents::Byte(k) => k.clear(),
            ScalarPlainEvents::Short(k) => k.clear(),
            ScalarPlainEvents::Int(k) => k.clear(),
            ScalarPlainEvents::Float(k) => k.clear(),
            ScalarPlainEvents::Double(k) => k.clear(),
        }
    }
}

impl Appendable for ScalarPlainEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::Byte(k) => Self::Byte(k.empty_like_self()),
            Self::Short(k) => Self::Short(k.empty_like_self()),
            Self::Int(k) => Self::Int(k.empty_like_self()),
            Self::Float(k) => Self::Float(k.empty_like_self()),
            Self::Double(k) => Self::Double(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.append(j),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.append(j),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.append(j),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.append(j),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for ScalarPlainEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for ScalarPlainEvents {
    fn len(&self) -> usize {
        use ScalarPlainEvents::*;
        match self {
            Byte(j) => j.len(),
            Short(j) => j.len(),
            Int(j) => j.len(),
            Float(j) => j.len(),
            Double(j) => j.len(),
        }
    }
}

impl WithTimestamps for ScalarPlainEvents {
    fn ts(&self, ix: usize) -> u64 {
        use ScalarPlainEvents::*;
        match self {
            Byte(j) => j.ts(ix),
            Short(j) => j.ts(ix),
            Int(j) => j.ts(ix),
            Float(j) => j.ts(ix),
            Double(j) => j.ts(ix),
        }
    }
}

impl HasShape for ScalarPlainEvents {
    fn shape(&self) -> Shape {
        match self {
            _ => Shape::Scalar,
        }
    }
}

impl HasScalarType for ScalarPlainEvents {
    fn scalar_type(&self) -> ScalarType {
        use ScalarPlainEvents::*;
        match self {
            Byte(_) => ScalarType::I8,
            Short(_) => ScalarType::I16,
            Int(_) => ScalarType::I32,
            Float(_) => ScalarType::F32,
            Double(_) => ScalarType::F64,
        }
    }
}

#[derive(Debug)]
pub enum WavePlainEvents {
    Byte(WaveEvents<i8>),
    Short(WaveEvents<i16>),
    Int(WaveEvents<i32>),
    Float(WaveEvents<f32>),
    Double(WaveEvents<f64>),
}

impl WavePlainEvents {
    pub fn shape(&self) -> Result<Shape, Error> {
        match self {
            WavePlainEvents::Byte(k) => k.shape(),
            WavePlainEvents::Short(k) => k.shape(),
            WavePlainEvents::Int(k) => k.shape(),
            WavePlainEvents::Float(k) => k.shape(),
            WavePlainEvents::Double(k) => k.shape(),
        }
    }
}

fn _tmp1() {
    let _ev = EventValues::<u8> {
        tss: vec![],
        values: vec![],
    };
    <u8 as NumOps>::is_nan(err::todoval());
    if <EventValues<u8> as SitemtyFrameType>::FRAME_TYPE_ID == 0 {
        // Just a dummy..
        panic!();
    }
    //<Vec<u8> as NumOps>::is_nan(err::todoval());
    //<EventValues<Vec<u8>> as SitemtyFrameType>::FRAME_TYPE_ID;
}

macro_rules! wagg1 {
    ($k:expr, $ak:expr, $shape:expr, $sty:ident) => {
        match $ak {
            AggKind::EventBlobs => panic!(),
            AggKind::Plain => EventsItem::Plain(PlainEvents::Wave(WavePlainEvents::$sty($k))),
            AggKind::TimeWeightedScalar => {
                let p = WaveXBinner::create($shape, $ak.clone());
                let j = p.process($k);
                EventsItem::XBinnedEvents(XBinnedEvents::SingleBinWave(SingleBinWaveEvents::$sty(j)))
            }
            AggKind::DimXBins1 => {
                let p = WaveXBinner::create($shape, $ak.clone());
                let j = p.process($k);
                EventsItem::XBinnedEvents(XBinnedEvents::SingleBinWave(SingleBinWaveEvents::$sty(j)))
            }
            AggKind::DimXBinsN(_) => EventsItem::Plain(PlainEvents::Wave(err::todoval())),
        }
    };
}

impl WavePlainEvents {
    pub fn variant_name(&self) -> String {
        use WavePlainEvents::*;
        match self {
            Byte(h) => format!("Byte({})", h.vals.first().map_or(0, |j| j.len())),
            Short(h) => format!("Short({})", h.vals.first().map_or(0, |j| j.len())),
            Int(h) => format!("Int({})", h.vals.first().map_or(0, |j| j.len())),
            Float(h) => format!("Float({})", h.vals.first().map_or(0, |j| j.len())),
            Double(h) => format!("Double({})", h.vals.first().map_or(0, |j| j.len())),
        }
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use WavePlainEvents::*;
        let shape = self.shape().unwrap();
        match self {
            Byte(k) => wagg1!(k, ak, shape, Byte),
            Short(k) => wagg1!(k, ak, shape, Short),
            Int(k) => wagg1!(k, ak, shape, Int),
            Float(k) => wagg1!(k, ak, shape, Float),
            Double(k) => wagg1!(k, ak, shape, Double),
        }
    }
}

impl Clearable for WavePlainEvents {
    fn clear(&mut self) {
        match self {
            WavePlainEvents::Byte(k) => k.clear(),
            WavePlainEvents::Short(k) => k.clear(),
            WavePlainEvents::Int(k) => k.clear(),
            WavePlainEvents::Float(k) => k.clear(),
            WavePlainEvents::Double(k) => k.clear(),
        }
    }
}

impl Appendable for WavePlainEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::Byte(k) => Self::Byte(k.empty_like_self()),
            Self::Short(k) => Self::Short(k.empty_like_self()),
            Self::Int(k) => Self::Int(k.empty_like_self()),
            Self::Float(k) => Self::Float(k.empty_like_self()),
            Self::Double(k) => Self::Double(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.append(j),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.append(j),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.append(j),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.append(j),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for WavePlainEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for WavePlainEvents {
    fn len(&self) -> usize {
        use WavePlainEvents::*;
        match self {
            Byte(j) => j.len(),
            Short(j) => j.len(),
            Int(j) => j.len(),
            Float(j) => j.len(),
            Double(j) => j.len(),
        }
    }
}

impl WithTimestamps for WavePlainEvents {
    fn ts(&self, ix: usize) -> u64 {
        use WavePlainEvents::*;
        match self {
            Byte(j) => j.ts(ix),
            Short(j) => j.ts(ix),
            Int(j) => j.ts(ix),
            Float(j) => j.ts(ix),
            Double(j) => j.ts(ix),
        }
    }
}

impl HasShape for WavePlainEvents {
    fn shape(&self) -> Shape {
        /*use WavePlainEvents::*;
        match self {
            Byte(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
            Short(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
            Int(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
            Float(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
            Double(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
        }*/
        self.shape().unwrap()
    }
}

impl HasScalarType for WavePlainEvents {
    fn scalar_type(&self) -> ScalarType {
        use WavePlainEvents::*;
        match self {
            Byte(_) => ScalarType::I8,
            Short(_) => ScalarType::I16,
            Int(_) => ScalarType::I32,
            Float(_) => ScalarType::F32,
            Double(_) => ScalarType::F64,
        }
    }
}

#[derive(Debug)]
pub enum MultiBinWaveEvents {
    Byte(XBinnedWaveEvents<i8>),
    Short(XBinnedWaveEvents<i16>),
    Int(XBinnedWaveEvents<i32>),
    Float(XBinnedWaveEvents<f32>),
    Double(XBinnedWaveEvents<f64>),
}

impl MultiBinWaveEvents {
    pub fn variant_name(&self) -> String {
        use MultiBinWaveEvents::*;
        match self {
            Byte(_) => format!("Byte"),
            Short(_) => format!("Short"),
            Int(_) => format!("Int"),
            Float(_) => format!("Float"),
            Double(_) => format!("Double"),
        }
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use MultiBinWaveEvents::*;
        match self {
            Byte(k) => match ak {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => EventsItem::XBinnedEvents(XBinnedEvents::MultiBinWave(MultiBinWaveEvents::Byte(k))),
                AggKind::TimeWeightedScalar => err::todoval(),
                AggKind::DimXBins1 => err::todoval(),
                AggKind::DimXBinsN(_) => EventsItem::Plain(PlainEvents::Wave(err::todoval())),
            },
            _ => err::todoval(),
        }
    }
}

impl Clearable for MultiBinWaveEvents {
    fn clear(&mut self) {
        match self {
            MultiBinWaveEvents::Byte(k) => k.clear(),
            MultiBinWaveEvents::Short(k) => k.clear(),
            MultiBinWaveEvents::Int(k) => k.clear(),
            MultiBinWaveEvents::Float(k) => k.clear(),
            MultiBinWaveEvents::Double(k) => k.clear(),
        }
    }
}

impl Appendable for MultiBinWaveEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::Byte(k) => Self::Byte(k.empty_like_self()),
            Self::Short(k) => Self::Short(k.empty_like_self()),
            Self::Int(k) => Self::Int(k.empty_like_self()),
            Self::Float(k) => Self::Float(k.empty_like_self()),
            Self::Double(k) => Self::Double(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.append(j),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.append(j),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.append(j),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.append(j),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for MultiBinWaveEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for MultiBinWaveEvents {
    fn len(&self) -> usize {
        use MultiBinWaveEvents::*;
        match self {
            Byte(j) => j.len(),
            Short(j) => j.len(),
            Int(j) => j.len(),
            Float(j) => j.len(),
            Double(j) => j.len(),
        }
    }
}

impl WithTimestamps for MultiBinWaveEvents {
    fn ts(&self, ix: usize) -> u64 {
        use MultiBinWaveEvents::*;
        match self {
            Byte(j) => j.ts(ix),
            Short(j) => j.ts(ix),
            Int(j) => j.ts(ix),
            Float(j) => j.ts(ix),
            Double(j) => j.ts(ix),
        }
    }
}

impl HasShape for MultiBinWaveEvents {
    fn shape(&self) -> Shape {
        use MultiBinWaveEvents::*;
        match self {
            Byte(_) => Shape::Scalar,
            Short(_) => Shape::Scalar,
            Int(_) => Shape::Scalar,
            Float(_) => Shape::Scalar,
            Double(_) => Shape::Scalar,
        }
    }
}

impl HasScalarType for MultiBinWaveEvents {
    fn scalar_type(&self) -> ScalarType {
        use MultiBinWaveEvents::*;
        match self {
            Byte(_) => ScalarType::I8,
            Short(_) => ScalarType::I16,
            Int(_) => ScalarType::I32,
            Float(_) => ScalarType::F32,
            Double(_) => ScalarType::F64,
        }
    }
}

#[derive(Debug)]
pub enum SingleBinWaveEvents {
    Byte(XBinnedScalarEvents<i8>),
    Short(XBinnedScalarEvents<i16>),
    Int(XBinnedScalarEvents<i32>),
    Float(XBinnedScalarEvents<f32>),
    Double(XBinnedScalarEvents<f64>),
}

impl SingleBinWaveEvents {
    pub fn variant_name(&self) -> String {
        use SingleBinWaveEvents::*;
        match self {
            Byte(_) => format!("Byte"),
            Short(_) => format!("Short"),
            Int(_) => format!("Int"),
            Float(_) => format!("Float"),
            Double(_) => format!("Double"),
        }
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use SingleBinWaveEvents::*;
        match self {
            Byte(k) => match ak {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => EventsItem::XBinnedEvents(XBinnedEvents::SingleBinWave(SingleBinWaveEvents::Byte(k))),
                AggKind::TimeWeightedScalar => err::todoval(),
                AggKind::DimXBins1 => err::todoval(),
                AggKind::DimXBinsN(_) => EventsItem::Plain(PlainEvents::Wave(err::todoval())),
            },
            _ => err::todoval(),
        }
    }
}

impl Clearable for SingleBinWaveEvents {
    fn clear(&mut self) {
        match self {
            SingleBinWaveEvents::Byte(k) => k.clear(),
            SingleBinWaveEvents::Short(k) => k.clear(),
            SingleBinWaveEvents::Int(k) => k.clear(),
            SingleBinWaveEvents::Float(k) => k.clear(),
            SingleBinWaveEvents::Double(k) => k.clear(),
        }
    }
}

impl Appendable for SingleBinWaveEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::Byte(k) => Self::Byte(k.empty_like_self()),
            Self::Short(k) => Self::Short(k.empty_like_self()),
            Self::Int(k) => Self::Int(k.empty_like_self()),
            Self::Float(k) => Self::Float(k.empty_like_self()),
            Self::Double(k) => Self::Double(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.append(j),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.append(j),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.append(j),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.append(j),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for SingleBinWaveEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::Byte(k) => match src {
                Self::Byte(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Short(k) => match src {
                Self::Short(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Int(k) => match src {
                Self::Int(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Float(k) => match src {
                Self::Float(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Double(k) => match src {
                Self::Double(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for SingleBinWaveEvents {
    fn len(&self) -> usize {
        use SingleBinWaveEvents::*;
        match self {
            Byte(j) => j.len(),
            Short(j) => j.len(),
            Int(j) => j.len(),
            Float(j) => j.len(),
            Double(j) => j.len(),
        }
    }
}

impl WithTimestamps for SingleBinWaveEvents {
    fn ts(&self, ix: usize) -> u64 {
        use SingleBinWaveEvents::*;
        match self {
            Byte(j) => j.ts(ix),
            Short(j) => j.ts(ix),
            Int(j) => j.ts(ix),
            Float(j) => j.ts(ix),
            Double(j) => j.ts(ix),
        }
    }
}

impl HasShape for SingleBinWaveEvents {
    fn shape(&self) -> Shape {
        use SingleBinWaveEvents::*;
        match self {
            Byte(_) => Shape::Scalar,
            Short(_) => Shape::Scalar,
            Int(_) => Shape::Scalar,
            Float(_) => Shape::Scalar,
            Double(_) => Shape::Scalar,
        }
    }
}

impl HasScalarType for SingleBinWaveEvents {
    fn scalar_type(&self) -> ScalarType {
        use SingleBinWaveEvents::*;
        match self {
            Byte(_) => ScalarType::I8,
            Short(_) => ScalarType::I16,
            Int(_) => ScalarType::I32,
            Float(_) => ScalarType::F32,
            Double(_) => ScalarType::F64,
        }
    }
}

#[derive(Debug)]
pub enum XBinnedEvents {
    Scalar(ScalarPlainEvents),
    SingleBinWave(SingleBinWaveEvents),
    MultiBinWave(MultiBinWaveEvents),
}

impl XBinnedEvents {
    pub fn variant_name(&self) -> String {
        use XBinnedEvents::*;
        match self {
            Scalar(h) => format!("Scalar({})", h.variant_name()),
            SingleBinWave(h) => format!("SingleBinWave({})", h.variant_name()),
            MultiBinWave(h) => format!("MultiBinWave({})", h.variant_name()),
        }
    }

    pub fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use XBinnedEvents::*;
        match self {
            Scalar(k) => EventsItem::Plain(PlainEvents::Scalar(k)),
            SingleBinWave(k) => k.x_aggregate(ak),
            MultiBinWave(k) => k.x_aggregate(ak),
        }
    }
}

impl Clearable for XBinnedEvents {
    fn clear(&mut self) {
        match self {
            XBinnedEvents::Scalar(k) => k.clear(),
            XBinnedEvents::SingleBinWave(k) => k.clear(),
            XBinnedEvents::MultiBinWave(k) => k.clear(),
        }
    }
}

impl Appendable for XBinnedEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::Scalar(k) => Self::Scalar(k.empty_like_self()),
            Self::SingleBinWave(k) => Self::SingleBinWave(k.empty_like_self()),
            Self::MultiBinWave(k) => Self::MultiBinWave(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::Scalar(k) => match src {
                Self::Scalar(j) => k.append(j),
                _ => panic!(),
            },
            Self::SingleBinWave(k) => match src {
                Self::SingleBinWave(j) => k.append(j),
                _ => panic!(),
            },
            Self::MultiBinWave(k) => match src {
                Self::MultiBinWave(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for XBinnedEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::Scalar(k) => match src {
                Self::Scalar(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::SingleBinWave(k) => match src {
                Self::SingleBinWave(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::MultiBinWave(k) => match src {
                Self::MultiBinWave(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for XBinnedEvents {
    fn len(&self) -> usize {
        use XBinnedEvents::*;
        match self {
            Scalar(j) => j.len(),
            SingleBinWave(j) => j.len(),
            MultiBinWave(j) => j.len(),
        }
    }
}

impl WithTimestamps for XBinnedEvents {
    fn ts(&self, ix: usize) -> u64 {
        use XBinnedEvents::*;
        match self {
            Scalar(j) => j.ts(ix),
            SingleBinWave(j) => j.ts(ix),
            MultiBinWave(j) => j.ts(ix),
        }
    }
}

impl HasShape for XBinnedEvents {
    fn shape(&self) -> Shape {
        use XBinnedEvents::*;
        match self {
            Scalar(h) => h.shape(),
            SingleBinWave(h) => h.shape(),
            MultiBinWave(h) => h.shape(),
        }
    }
}

impl HasScalarType for XBinnedEvents {
    fn scalar_type(&self) -> ScalarType {
        use XBinnedEvents::*;
        match self {
            Scalar(h) => h.scalar_type(),
            SingleBinWave(h) => h.scalar_type(),
            MultiBinWave(h) => h.scalar_type(),
        }
    }
}

#[derive(Debug)]
pub enum PlainEvents {
    Scalar(ScalarPlainEvents),
    Wave(WavePlainEvents),
}

impl PlainEvents {
    pub fn is_wave(&self) -> bool {
        use PlainEvents::*;
        match self {
            Scalar(_) => false,
            Wave(_) => true,
        }
    }

    pub fn variant_name(&self) -> String {
        use PlainEvents::*;
        match self {
            Scalar(h) => format!("Scalar({})", h.variant_name()),
            Wave(h) => format!("Scalar({})", h.variant_name()),
        }
    }

    pub fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use PlainEvents::*;
        match self {
            Scalar(k) => EventsItem::Plain(PlainEvents::Scalar(k)),
            Wave(k) => k.x_aggregate(ak),
        }
    }
}

impl Clearable for PlainEvents {
    fn clear(&mut self) {
        match self {
            PlainEvents::Scalar(k) => k.clear(),
            PlainEvents::Wave(k) => k.clear(),
        }
    }
}

impl Appendable for PlainEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::Scalar(k) => Self::Scalar(k.empty_like_self()),
            Self::Wave(k) => Self::Wave(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            PlainEvents::Scalar(k) => match src {
                Self::Scalar(j) => k.append(j),
                _ => panic!(),
            },
            PlainEvents::Wave(k) => match src {
                Self::Wave(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for PlainEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::Scalar(k) => match src {
                Self::Scalar(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::Wave(k) => match src {
                Self::Wave(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for PlainEvents {
    fn len(&self) -> usize {
        use PlainEvents::*;
        match self {
            Scalar(j) => j.len(),
            Wave(j) => j.len(),
        }
    }
}

impl WithTimestamps for PlainEvents {
    fn ts(&self, ix: usize) -> u64 {
        use PlainEvents::*;
        match self {
            Scalar(j) => j.ts(ix),
            Wave(j) => j.ts(ix),
        }
    }
}

impl HasShape for PlainEvents {
    fn shape(&self) -> Shape {
        use PlainEvents::*;
        match self {
            Scalar(h) => HasShape::shape(h),
            Wave(h) => HasShape::shape(h),
        }
    }
}

impl HasScalarType for PlainEvents {
    fn scalar_type(&self) -> ScalarType {
        use PlainEvents::*;
        match self {
            Scalar(h) => h.scalar_type(),
            Wave(h) => h.scalar_type(),
        }
    }
}

#[derive(Debug)]
pub enum EventsItem {
    Plain(PlainEvents),
    XBinnedEvents(XBinnedEvents),
}

impl EventsItem {
    pub fn is_wave(&self) -> bool {
        use EventsItem::*;
        match self {
            Plain(h) => h.is_wave(),
            XBinnedEvents(h) => {
                if let Shape::Wave(_) = h.shape() {
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn variant_name(&self) -> String {
        use EventsItem::*;
        match self {
            Plain(h) => format!("Plain({})", h.variant_name()),
            XBinnedEvents(h) => format!("Plain({})", h.variant_name()),
        }
    }

    pub fn x_aggregate(self, ak: &AggKind) -> Self {
        use EventsItem::*;
        match self {
            Plain(k) => k.x_aggregate(ak),
            XBinnedEvents(k) => k.x_aggregate(ak),
        }
    }

    pub fn type_info(&self) -> (ScalarType, Shape) {
        match self {
            EventsItem::Plain(k) => match k {
                PlainEvents::Scalar(k) => match k {
                    ScalarPlainEvents::Byte(_) => (ScalarType::I8, Shape::Scalar),
                    ScalarPlainEvents::Short(_) => (ScalarType::I16, Shape::Scalar),
                    ScalarPlainEvents::Int(_) => (ScalarType::I32, Shape::Scalar),
                    ScalarPlainEvents::Float(_) => (ScalarType::F32, Shape::Scalar),
                    ScalarPlainEvents::Double(_) => (ScalarType::F64, Shape::Scalar),
                },
                PlainEvents::Wave(k) => match k {
                    // TODO
                    // Inherent issue for the non-static-type backends:
                    // there is a chance that we can't determine the shape here.
                    WavePlainEvents::Byte(k) => (ScalarType::I8, k.shape().unwrap()),
                    WavePlainEvents::Short(k) => (ScalarType::I16, k.shape().unwrap()),
                    WavePlainEvents::Int(k) => (ScalarType::I32, k.shape().unwrap()),
                    WavePlainEvents::Float(k) => (ScalarType::F32, k.shape().unwrap()),
                    WavePlainEvents::Double(k) => (ScalarType::F64, k.shape().unwrap()),
                },
            },
            EventsItem::XBinnedEvents(k) => panic!(),
        }
    }
}

impl WithLen for EventsItem {
    fn len(&self) -> usize {
        use EventsItem::*;
        match self {
            Plain(j) => j.len(),
            XBinnedEvents(j) => j.len(),
        }
    }
}

impl WithTimestamps for EventsItem {
    fn ts(&self, ix: usize) -> u64 {
        use EventsItem::*;
        match self {
            Plain(j) => j.ts(ix),
            XBinnedEvents(j) => j.ts(ix),
        }
    }
}

impl Appendable for EventsItem {
    fn empty_like_self(&self) -> Self {
        match self {
            EventsItem::Plain(k) => EventsItem::Plain(k.empty_like_self()),
            EventsItem::XBinnedEvents(k) => EventsItem::XBinnedEvents(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::Plain(k) => match src {
                Self::Plain(j) => k.append(j),
                _ => panic!(),
            },
            Self::XBinnedEvents(k) => match src {
                Self::XBinnedEvents(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for EventsItem {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::Plain(k) => match src {
                Self::Plain(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::XBinnedEvents(k) => match src {
                Self::XBinnedEvents(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl Clearable for EventsItem {
    fn clear(&mut self) {
        match self {
            EventsItem::Plain(k) => k.clear(),
            EventsItem::XBinnedEvents(k) => k.clear(),
        }
    }
}

impl HasShape for EventsItem {
    fn shape(&self) -> Shape {
        use EventsItem::*;
        match self {
            Plain(h) => h.shape(),
            XBinnedEvents(h) => h.shape(),
        }
    }
}

impl HasScalarType for EventsItem {
    fn scalar_type(&self) -> ScalarType {
        use EventsItem::*;
        match self {
            Plain(h) => h.scalar_type(),
            XBinnedEvents(h) => h.scalar_type(),
        }
    }
}
