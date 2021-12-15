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
    I8(XBinnedScalarEvents<i8>),
    I16(XBinnedScalarEvents<i16>),
    I32(XBinnedScalarEvents<i32>),
    F32(XBinnedScalarEvents<f32>),
    F64(XBinnedScalarEvents<f64>),
}

impl SingleBinWaveEvents {
    pub fn variant_name(&self) -> String {
        use SingleBinWaveEvents::*;
        match self {
            I8(_) => format!("I8"),
            I16(_) => format!("I16"),
            I32(_) => format!("I32"),
            F32(_) => format!("F32"),
            F64(_) => format!("F64"),
        }
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use SingleBinWaveEvents::*;
        match self {
            I8(k) => match ak {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => EventsItem::XBinnedEvents(XBinnedEvents::SingleBinWave(SingleBinWaveEvents::I8(k))),
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
            SingleBinWaveEvents::I8(k) => k.clear(),
            SingleBinWaveEvents::I16(k) => k.clear(),
            SingleBinWaveEvents::I32(k) => k.clear(),
            SingleBinWaveEvents::F32(k) => k.clear(),
            SingleBinWaveEvents::F64(k) => k.clear(),
        }
    }
}

impl Appendable for SingleBinWaveEvents {
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

impl PushableIndex for SingleBinWaveEvents {
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

impl WithLen for SingleBinWaveEvents {
    fn len(&self) -> usize {
        use SingleBinWaveEvents::*;
        match self {
            I8(j) => j.len(),
            I16(j) => j.len(),
            I32(j) => j.len(),
            F32(j) => j.len(),
            F64(j) => j.len(),
        }
    }
}

impl WithTimestamps for SingleBinWaveEvents {
    fn ts(&self, ix: usize) -> u64 {
        use SingleBinWaveEvents::*;
        match self {
            I8(j) => j.ts(ix),
            I16(j) => j.ts(ix),
            I32(j) => j.ts(ix),
            F32(j) => j.ts(ix),
            F64(j) => j.ts(ix),
        }
    }
}

impl HasShape for SingleBinWaveEvents {
    fn shape(&self) -> Shape {
        use SingleBinWaveEvents::*;
        match self {
            I8(_) => Shape::Scalar,
            I16(_) => Shape::Scalar,
            I32(_) => Shape::Scalar,
            F32(_) => Shape::Scalar,
            F64(_) => Shape::Scalar,
        }
    }
}

impl HasScalarType for SingleBinWaveEvents {
    fn scalar_type(&self) -> ScalarType {
        use SingleBinWaveEvents::*;
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
pub enum MultiBinWaveEvents {
    I8(XBinnedWaveEvents<i8>),
    I16(XBinnedWaveEvents<i16>),
    I32(XBinnedWaveEvents<i32>),
    F32(XBinnedWaveEvents<f32>),
    F64(XBinnedWaveEvents<f64>),
}

impl MultiBinWaveEvents {
    pub fn variant_name(&self) -> String {
        use MultiBinWaveEvents::*;
        match self {
            I8(_) => format!("I8"),
            I16(_) => format!("I16"),
            I32(_) => format!("I32"),
            F32(_) => format!("F32"),
            F64(_) => format!("F64"),
        }
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        use MultiBinWaveEvents::*;
        match self {
            I8(k) => match ak {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => EventsItem::XBinnedEvents(XBinnedEvents::MultiBinWave(MultiBinWaveEvents::I8(k))),
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
            MultiBinWaveEvents::I8(k) => k.clear(),
            MultiBinWaveEvents::I16(k) => k.clear(),
            MultiBinWaveEvents::I32(k) => k.clear(),
            MultiBinWaveEvents::F32(k) => k.clear(),
            MultiBinWaveEvents::F64(k) => k.clear(),
        }
    }
}

impl Appendable for MultiBinWaveEvents {
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

impl PushableIndex for MultiBinWaveEvents {
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

impl WithLen for MultiBinWaveEvents {
    fn len(&self) -> usize {
        use MultiBinWaveEvents::*;
        match self {
            I8(j) => j.len(),
            I16(j) => j.len(),
            I32(j) => j.len(),
            F32(j) => j.len(),
            F64(j) => j.len(),
        }
    }
}

impl WithTimestamps for MultiBinWaveEvents {
    fn ts(&self, ix: usize) -> u64 {
        use MultiBinWaveEvents::*;
        match self {
            I8(j) => j.ts(ix),
            I16(j) => j.ts(ix),
            I32(j) => j.ts(ix),
            F32(j) => j.ts(ix),
            F64(j) => j.ts(ix),
        }
    }
}

impl HasShape for MultiBinWaveEvents {
    fn shape(&self) -> Shape {
        use MultiBinWaveEvents::*;
        match self {
            I8(_) => Shape::Scalar,
            I16(_) => Shape::Scalar,
            I32(_) => Shape::Scalar,
            F32(_) => Shape::Scalar,
            F64(_) => Shape::Scalar,
        }
    }
}

impl HasScalarType for MultiBinWaveEvents {
    fn scalar_type(&self) -> ScalarType {
        use MultiBinWaveEvents::*;
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
