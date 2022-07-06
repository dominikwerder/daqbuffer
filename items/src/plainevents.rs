use crate::binnedevents::{SingleBinWaveEvents, XBinnedEvents};
use crate::eventsitem::EventsItem;
use crate::scalarevents::ScalarEvents;
use crate::waveevents::{WaveEvents, WaveXBinner};
use crate::xbinnedscalarevents::XBinnedScalarEvents;
use crate::{Appendable, Clearable, EventsNodeProcessor, PushableIndex, WithLen, WithTimestamps};
use err::Error;
use netpod::{AggKind, HasScalarType, HasShape, ScalarType, Shape};
use serde::{Deserialize, Serialize};

//items_proc::enumvars!(ScalarPlainEvents, EventValues);

#[derive(Debug, Serialize, Deserialize)]
pub enum ScalarPlainEvents {
    U8(ScalarEvents<u8>),
    U16(ScalarEvents<u16>),
    U32(ScalarEvents<u32>),
    U64(ScalarEvents<u64>),
    I8(ScalarEvents<i8>),
    I16(ScalarEvents<i16>),
    I32(ScalarEvents<i32>),
    I64(ScalarEvents<i64>),
    F32(ScalarEvents<f32>),
    F64(ScalarEvents<f64>),
    String(ScalarEvents<String>),
}

impl ScalarPlainEvents {
    pub fn variant_name(&self) -> String {
        items_proc::tycases1!(self, Self, (k), { "$id".into() })
    }
}

impl Clearable for ScalarPlainEvents {
    fn clear(&mut self) {
        items_proc::tycases1!(self, Self, (k), { k.clear() })
    }
}

impl Appendable for ScalarPlainEvents {
    fn empty_like_self(&self) -> Self {
        items_proc::tycases1!(self, Self, (k), { Self::$id(k.empty_like_self()) })
    }

    fn append(&mut self, src: &Self) {
        items_proc::tycases1!(self, Self, (k), {
            match src {
                Self::$id(j) => k.append(j),
                _ => panic!(),
            }
        })
    }

    fn append_zero(&mut self, _ts1: u64, _ts2: u64) {
        // TODO can this implement Appendable in a sane way? Do we need it?
        err::todo();
    }
}

impl PushableIndex for ScalarPlainEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        items_proc::tycases1!(self, Self, (k), {
            match src {
                Self::$id(j) => k.push_index(j, ix),
                _ => panic!(),
            }
        })
    }
}

impl WithLen for ScalarPlainEvents {
    fn len(&self) -> usize {
        items_proc::tycases1!(self, Self, (k), { k.len() })
    }
}

impl WithTimestamps for ScalarPlainEvents {
    fn ts(&self, ix: usize) -> u64 {
        items_proc::tycases1!(self, Self, (k), { k.ts(ix) })
    }
}

impl HasShape for ScalarPlainEvents {
    fn shape(&self) -> Shape {
        Shape::Scalar
    }
}

impl HasScalarType for ScalarPlainEvents {
    fn scalar_type(&self) -> ScalarType {
        items_proc::tycases1!(self, Self, (k), { ScalarType::$id })
    }
}

//items_proc::enumvars!(WavePlainEvents, WaveEvents);

#[derive(Debug, Serialize, Deserialize)]
pub enum WavePlainEvents {
    U8(WaveEvents<u8>),
    U16(WaveEvents<u16>),
    U32(WaveEvents<u32>),
    U64(WaveEvents<u64>),
    I8(WaveEvents<i8>),
    I16(WaveEvents<i16>),
    I32(WaveEvents<i32>),
    I64(WaveEvents<i64>),
    F32(WaveEvents<f32>),
    F64(WaveEvents<f64>),
    String(WaveEvents<String>),
}

impl WavePlainEvents {
    pub fn shape(&self) -> Result<Shape, Error> {
        items_proc::tycases1!(self, Self, (k), { k.shape() })
    }
}

impl WavePlainEvents {
    pub fn variant_name(&self) -> String {
        items_proc::tycases1!(self, Self, (k), {
            format!("$id({})", k.vals.first().map_or(0, |j| j.len()))
        })
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        let shape = self.shape().unwrap();
        items_proc::tycases1!(self, Self, (k), {
            match ak {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => EventsItem::Plain(PlainEvents::Wave(WavePlainEvents::$id(k))),
                AggKind::TimeWeightedScalar => {
                    let p = WaveXBinner::<$ty>::create(shape, ak.clone());
                    let j: XBinnedScalarEvents<$ty> = p.process(k);
                    EventsItem::XBinnedEvents(XBinnedEvents::SingleBinWave(SingleBinWaveEvents::$id(j)))
                }
                AggKind::DimXBins1 => {
                    let p = WaveXBinner::<$ty>::create(shape, ak.clone());
                    let j: XBinnedScalarEvents<$ty> = p.process(k);
                    EventsItem::XBinnedEvents(XBinnedEvents::SingleBinWave(SingleBinWaveEvents::$id(j)))
                }
                AggKind::DimXBinsN(_) => EventsItem::Plain(PlainEvents::Wave(err::todoval())),
                AggKind::Stats1 => err::todoval(),
            }
        })
    }
}

impl Clearable for WavePlainEvents {
    fn clear(&mut self) {
        items_proc::tycases1!(self, Self, (k), { k.clear() })
    }
}

impl Appendable for WavePlainEvents {
    fn empty_like_self(&self) -> Self {
        items_proc::tycases1!(self, Self, (k), { Self::$id(k.empty_like_self()) })
    }

    fn append(&mut self, src: &Self) {
        items_proc::tycases1!(self, Self, (k), { match src {
            Self::$id(j) => k.append(j),
            _ => panic!(),
        } })
    }

    fn append_zero(&mut self, _ts1: u64, _ts2: u64) {
        // TODO can this implement Appendable in a sane way? Do we need it?
        err::todo();
    }
}

impl PushableIndex for WavePlainEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        items_proc::tycases1!(self, Self, (k), { match src {
            Self::$id(j) => k.push_index(j, ix),
            _ => panic!(),
        } })
    }
}

impl WithLen for WavePlainEvents {
    fn len(&self) -> usize {
        items_proc::tycases1!(self, Self, (k), { k.len() })
    }
}

impl WithTimestamps for WavePlainEvents {
    fn ts(&self, ix: usize) -> u64 {
        items_proc::tycases1!(self, Self, (k), { k.ts(ix) })
    }
}

impl HasShape for WavePlainEvents {
    fn shape(&self) -> Shape {
        self.shape().unwrap()
    }
}

impl HasScalarType for WavePlainEvents {
    fn scalar_type(&self) -> ScalarType {
        items_proc::tycases1!(self, Self, (k), { ScalarType::$id })
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

    fn append_zero(&mut self, _ts1: u64, _ts2: u64) {
        // TODO can this implement Appendable in a sane way? Do we need it?
        err::todo();
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
