use crate::binnedevents::XBinnedEvents;
use crate::plainevents::PlainEvents;
use crate::{Appendable, Clearable, PushableIndex, SitemtyFrameType, WithLen, WithTimestamps};
use netpod::{AggKind, HasScalarType, HasShape, ScalarType, Shape};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum EventsItem {
    Plain(PlainEvents),
    XBinnedEvents(XBinnedEvents),
}

impl SitemtyFrameType for EventsItem {
    //const FRAME_TYPE_ID: u32 = crate::EVENTS_ITEM_FRAME_TYPE_ID;

    fn frame_type_id(&self) -> u32 {
        crate::EVENTS_ITEM_FRAME_TYPE_ID
    }
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
        (self.scalar_type(), self.shape())
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
