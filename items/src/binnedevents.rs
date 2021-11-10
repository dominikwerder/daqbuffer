use crate::xbinnedscalarevents::XBinnedScalarEvents;
use crate::xbinnedwaveevents::XBinnedWaveEvents;
use crate::{Appendable, Clearable, PushableIndex, WithLen, WithTimestamps};
use netpod::{AggKind, HasScalarType, HasShape, ScalarType, Shape};
use serde::{Deserialize, Serialize};

use crate::{
    eventsitem::EventsItem,
    plainevents::{PlainEvents, ScalarPlainEvents},
};

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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
