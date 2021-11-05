use crate::binnedevents::{SingleBinWaveEvents, XBinnedEvents};
use crate::eventsitem::EventsItem;
use crate::eventvalues::EventValues;
use crate::waveevents::{WaveEvents, WaveXBinner};
use crate::{Appendable, Clearable, EventsNodeProcessor, PushableIndex, WithLen, WithTimestamps};
use err::Error;
use netpod::{AggKind, HasScalarType, HasShape, ScalarType, Shape};

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
