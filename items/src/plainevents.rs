use crate::binnedevents::{SingleBinWaveEvents, XBinnedEvents};
use crate::eventsitem::EventsItem;
use crate::eventvalues::EventValues;
use crate::waveevents::{WaveEvents, WaveXBinner};
use crate::{Appendable, Clearable, EventsNodeProcessor, PushableIndex, WithLen, WithTimestamps};
use err::Error;
use netpod::{AggKind, HasScalarType, HasShape, ScalarType, Shape};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ScalarPlainEvents {
    U32(EventValues<u32>),
    I8(EventValues<i8>),
    I16(EventValues<i16>),
    I32(EventValues<i32>),
    F32(EventValues<f32>),
    F64(EventValues<f64>),
}

impl ScalarPlainEvents {
    pub fn variant_name(&self) -> String {
        use ScalarPlainEvents::*;
        match self {
            U32(_) => format!("U32"),
            I8(_) => format!("I8"),
            I16(_) => format!("I16"),
            I32(_) => format!("I32"),
            F32(_) => format!("F32"),
            F64(_) => format!("F64"),
        }
    }
}

impl Clearable for ScalarPlainEvents {
    fn clear(&mut self) {
        match self {
            ScalarPlainEvents::U32(k) => k.clear(),
            ScalarPlainEvents::I8(k) => k.clear(),
            ScalarPlainEvents::I16(k) => k.clear(),
            ScalarPlainEvents::I32(k) => k.clear(),
            ScalarPlainEvents::F32(k) => k.clear(),
            ScalarPlainEvents::F64(k) => k.clear(),
        }
    }
}

impl Appendable for ScalarPlainEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::U32(k) => Self::U32(k.empty_like_self()),
            Self::I8(k) => Self::I8(k.empty_like_self()),
            Self::I16(k) => Self::I16(k.empty_like_self()),
            Self::I32(k) => Self::I32(k.empty_like_self()),
            Self::F32(k) => Self::F32(k.empty_like_self()),
            Self::F64(k) => Self::F64(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::U32(k) => match src {
                Self::U32(j) => k.append(j),
                _ => panic!(),
            },
            Self::I8(k) => match src {
                Self::I8(j) => k.append(j),
                _ => panic!(),
            },
            Self::I16(k) => match src {
                Self::I16(j) => k.append(j),
                _ => panic!(),
            },
            Self::I32(k) => match src {
                Self::I32(j) => k.append(j),
                _ => panic!(),
            },
            Self::F32(k) => match src {
                Self::F32(j) => k.append(j),
                _ => panic!(),
            },
            Self::F64(k) => match src {
                Self::F64(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for ScalarPlainEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::U32(k) => match src {
                Self::U32(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::I8(k) => match src {
                Self::I8(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::I16(k) => match src {
                Self::I16(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::I32(k) => match src {
                Self::I32(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::F32(k) => match src {
                Self::F32(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::F64(k) => match src {
                Self::F64(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for ScalarPlainEvents {
    fn len(&self) -> usize {
        use ScalarPlainEvents::*;
        match self {
            U32(j) => j.len(),
            I8(j) => j.len(),
            I16(j) => j.len(),
            I32(j) => j.len(),
            F32(j) => j.len(),
            F64(j) => j.len(),
        }
    }
}

impl WithTimestamps for ScalarPlainEvents {
    fn ts(&self, ix: usize) -> u64 {
        use ScalarPlainEvents::*;
        match self {
            U32(j) => j.ts(ix),
            I8(j) => j.ts(ix),
            I16(j) => j.ts(ix),
            I32(j) => j.ts(ix),
            F32(j) => j.ts(ix),
            F64(j) => j.ts(ix),
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
            U32(_) => ScalarType::U32,
            I8(_) => ScalarType::I8,
            I16(_) => ScalarType::I16,
            I32(_) => ScalarType::I32,
            F32(_) => ScalarType::F32,
            F64(_) => ScalarType::F64,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WavePlainEvents {
    I8(WaveEvents<i8>),
    I16(WaveEvents<i16>),
    I32(WaveEvents<i32>),
    F32(WaveEvents<f32>),
    F64(WaveEvents<f64>),
}

impl WavePlainEvents {
    pub fn shape(&self) -> Result<Shape, Error> {
        match self {
            WavePlainEvents::I8(k) => k.shape(),
            WavePlainEvents::I16(k) => k.shape(),
            WavePlainEvents::I32(k) => k.shape(),
            WavePlainEvents::F32(k) => k.shape(),
            WavePlainEvents::F64(k) => k.shape(),
        }
    }
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
            I8(h) => format!("I8({})", h.vals.first().map_or(0, |j| j.len())),
            I16(h) => format!("I16({})", h.vals.first().map_or(0, |j| j.len())),
            I32(h) => format!("I32({})", h.vals.first().map_or(0, |j| j.len())),
            F32(h) => format!("F32({})", h.vals.first().map_or(0, |j| j.len())),
            F64(h) => format!("F64({})", h.vals.first().map_or(0, |j| j.len())),
        }
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use WavePlainEvents::*;
        let shape = self.shape().unwrap();
        match self {
            I8(k) => wagg1!(k, ak, shape, I8),
            I16(k) => wagg1!(k, ak, shape, I16),
            I32(k) => wagg1!(k, ak, shape, I32),
            F32(k) => wagg1!(k, ak, shape, F32),
            F64(k) => wagg1!(k, ak, shape, F64),
        }
    }
}

impl Clearable for WavePlainEvents {
    fn clear(&mut self) {
        match self {
            WavePlainEvents::I8(k) => k.clear(),
            WavePlainEvents::I16(k) => k.clear(),
            WavePlainEvents::I32(k) => k.clear(),
            WavePlainEvents::F32(k) => k.clear(),
            WavePlainEvents::F64(k) => k.clear(),
        }
    }
}

impl Appendable for WavePlainEvents {
    fn empty_like_self(&self) -> Self {
        match self {
            Self::I8(k) => Self::I8(k.empty_like_self()),
            Self::I16(k) => Self::I16(k.empty_like_self()),
            Self::I32(k) => Self::I32(k.empty_like_self()),
            Self::F32(k) => Self::F32(k.empty_like_self()),
            Self::F64(k) => Self::F64(k.empty_like_self()),
        }
    }

    fn append(&mut self, src: &Self) {
        match self {
            Self::I8(k) => match src {
                Self::I8(j) => k.append(j),
                _ => panic!(),
            },
            Self::I16(k) => match src {
                Self::I16(j) => k.append(j),
                _ => panic!(),
            },
            Self::I32(k) => match src {
                Self::I32(j) => k.append(j),
                _ => panic!(),
            },
            Self::F32(k) => match src {
                Self::F32(j) => k.append(j),
                _ => panic!(),
            },
            Self::F64(k) => match src {
                Self::F64(j) => k.append(j),
                _ => panic!(),
            },
        }
    }
}

impl PushableIndex for WavePlainEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        match self {
            Self::I8(k) => match src {
                Self::I8(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::I16(k) => match src {
                Self::I16(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::I32(k) => match src {
                Self::I32(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::F32(k) => match src {
                Self::F32(j) => k.push_index(j, ix),
                _ => panic!(),
            },
            Self::F64(k) => match src {
                Self::F64(j) => k.push_index(j, ix),
                _ => panic!(),
            },
        }
    }
}

impl WithLen for WavePlainEvents {
    fn len(&self) -> usize {
        use WavePlainEvents::*;
        match self {
            I8(j) => j.len(),
            I16(j) => j.len(),
            I32(j) => j.len(),
            F32(j) => j.len(),
            F64(j) => j.len(),
        }
    }
}

impl WithTimestamps for WavePlainEvents {
    fn ts(&self, ix: usize) -> u64 {
        use WavePlainEvents::*;
        match self {
            I8(j) => j.ts(ix),
            I16(j) => j.ts(ix),
            I32(j) => j.ts(ix),
            F32(j) => j.ts(ix),
            F64(j) => j.ts(ix),
        }
    }
}

impl HasShape for WavePlainEvents {
    fn shape(&self) -> Shape {
        /*use WavePlainEvents::*;
        match self {
            Byte(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
            I16(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
            I32(h) => Shape::Wave(h.vals.first().map_or(0, |x| x.len() as u32)),
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
            I8(_) => ScalarType::I8,
            I16(_) => ScalarType::I16,
            I32(_) => ScalarType::I32,
            F32(_) => ScalarType::F32,
            F64(_) => ScalarType::F64,
        }
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
