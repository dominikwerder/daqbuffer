use crate::eventsitem::EventsItem;
use crate::plainevents::{PlainEvents, ScalarPlainEvents};
use crate::xbinnedscalarevents::XBinnedScalarEvents;
use crate::xbinnedwaveevents::XBinnedWaveEvents;
use crate::{Appendable, Clearable, PushableIndex, WithLen, WithTimestamps};
use netpod::{AggKind, HasScalarType, HasShape, ScalarType, Shape};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum SingleBinWaveEvents {
    U8(XBinnedScalarEvents<u8>),
    U16(XBinnedScalarEvents<u16>),
    U32(XBinnedScalarEvents<u32>),
    U64(XBinnedScalarEvents<u64>),
    I8(XBinnedScalarEvents<i8>),
    I16(XBinnedScalarEvents<i16>),
    I32(XBinnedScalarEvents<i32>),
    I64(XBinnedScalarEvents<i64>),
    F32(XBinnedScalarEvents<f32>),
    F64(XBinnedScalarEvents<f64>),
}

impl SingleBinWaveEvents {
    pub fn variant_name(&self) -> String {
        items_proc::tycases1!(self, Self, (k), { "$id".into() })
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        items_proc::tycases1!(self, Self, (k), {
            match ak {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => EventsItem::XBinnedEvents(XBinnedEvents::SingleBinWave(SingleBinWaveEvents::$id(k))),
                AggKind::TimeWeightedScalar => err::todoval(),
                AggKind::DimXBins1 => err::todoval(),
                AggKind::DimXBinsN(_) => EventsItem::Plain(PlainEvents::Wave(err::todoval())),
            }
        })
    }
}

impl Clearable for SingleBinWaveEvents {
    fn clear(&mut self) {
        items_proc::tycases1!(self, Self, (k), { k.clear() })
    }
}

impl Appendable for SingleBinWaveEvents {
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
}

impl PushableIndex for SingleBinWaveEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        items_proc::tycases1!(self, Self, (k), {
            match src {
                Self::$id(j) => k.push_index(j, ix),
                _ => panic!(),
            }
        })
    }
}

impl WithLen for SingleBinWaveEvents {
    fn len(&self) -> usize {
        items_proc::tycases1!(self, Self, (k), { k.len() })
    }
}

impl WithTimestamps for SingleBinWaveEvents {
    fn ts(&self, ix: usize) -> u64 {
        items_proc::tycases1!(self, Self, (k), { k.ts(ix) })
    }
}

impl HasShape for SingleBinWaveEvents {
    fn shape(&self) -> Shape {
        Shape::Scalar
    }
}

impl HasScalarType for SingleBinWaveEvents {
    fn scalar_type(&self) -> ScalarType {
        items_proc::tycases1!(self, Self, (k), { ScalarType::$id })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MultiBinWaveEvents {
    U8(XBinnedWaveEvents<u8>),
    U16(XBinnedWaveEvents<u16>),
    U32(XBinnedWaveEvents<u32>),
    U64(XBinnedWaveEvents<u64>),
    I8(XBinnedWaveEvents<i8>),
    I16(XBinnedWaveEvents<i16>),
    I32(XBinnedWaveEvents<i32>),
    I64(XBinnedWaveEvents<i64>),
    F32(XBinnedWaveEvents<f32>),
    F64(XBinnedWaveEvents<f64>),
}

impl MultiBinWaveEvents {
    pub fn variant_name(&self) -> String {
        items_proc::tycases1!(self, Self, (k), { "$id".into() })
    }

    fn x_aggregate(self, ak: &AggKind) -> EventsItem {
        items_proc::tycases1!(self, Self, (k), {
            match ak {
                AggKind::EventBlobs => panic!(),
                AggKind::Plain => EventsItem::XBinnedEvents(XBinnedEvents::MultiBinWave(MultiBinWaveEvents::$id(k))),
                AggKind::TimeWeightedScalar => err::todoval(),
                AggKind::DimXBins1 => err::todoval(),
                AggKind::DimXBinsN(_) => EventsItem::Plain(PlainEvents::Wave(err::todoval())),
            }
        })
    }
}

impl Clearable for MultiBinWaveEvents {
    fn clear(&mut self) {
        items_proc::tycases1!(self, Self, (k), { k.clear() })
    }
}

impl Appendable for MultiBinWaveEvents {
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
}

impl PushableIndex for MultiBinWaveEvents {
    fn push_index(&mut self, src: &Self, ix: usize) {
        items_proc::tycases1!(self, Self, (k), {
            match src {
                Self::$id(j) => k.push_index(j, ix),
                _ => panic!(),
            }
        })
    }
}

impl WithLen for MultiBinWaveEvents {
    fn len(&self) -> usize {
        items_proc::tycases1!(self, Self, (k), { k.len() })
    }
}

impl WithTimestamps for MultiBinWaveEvents {
    fn ts(&self, ix: usize) -> u64 {
        items_proc::tycases1!(self, Self, (k), { k.ts(ix) })
    }
}

impl HasShape for MultiBinWaveEvents {
    fn shape(&self) -> Shape {
        Shape::Scalar
    }
}

impl HasScalarType for MultiBinWaveEvents {
    fn scalar_type(&self) -> ScalarType {
        items_proc::tycases1!(self, Self, (k), { ScalarType::$id })
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
