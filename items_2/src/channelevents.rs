use crate::merger;
use crate::merger_cev::MergeableCev;
use crate::Events;
use items::FrameType;
use items::FrameTypeInnerStatic;
use items_0::collect_s::Collectable;
use items_0::collect_s::Collector;
use netpod::log::*;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;

// TODO maybe rename to ChannelStatus?
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConnStatus {
    Connect,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnStatusEvent {
    pub ts: u64,
    pub status: ConnStatus,
}

impl ConnStatusEvent {
    pub fn new(ts: u64, status: ConnStatus) -> Self {
        Self { ts, status }
    }
}

/// Events on a channel consist not only of e.g. timestamped values, but can be also
/// connection status changes.
#[derive(Debug)]
pub enum ChannelEvents {
    Events(Box<dyn Events>),
    Status(ConnStatusEvent),
}

impl FrameTypeInnerStatic for ChannelEvents {
    const FRAME_TYPE_ID: u32 = items::ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID;
}

impl FrameType for ChannelEvents {
    fn frame_type_id(&self) -> u32 {
        // TODO SubFrId missing, but get rid of the frame type concept anyhow.
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

impl Clone for ChannelEvents {
    fn clone(&self) -> Self {
        match self {
            Self::Events(arg0) => Self::Events(arg0.clone_dyn()),
            Self::Status(arg0) => Self::Status(arg0.clone()),
        }
    }
}

mod serde_channel_events {
    use super::{ChannelEvents, Events};
    use crate::eventsdim0::EventsDim0;
    use serde::de::{self, EnumAccess, VariantAccess, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    impl Serialize for ChannelEvents {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let name = "ChannelEvents";
            match self {
                ChannelEvents::Events(obj) => {
                    use serde::ser::SerializeTupleVariant;
                    let mut ser = serializer.serialize_tuple_variant(name, 0, "Events", 3)?;
                    ser.serialize_field(obj.serde_id())?;
                    ser.serialize_field(&obj.nty_id())?;
                    ser.serialize_field(obj)?;
                    ser.end()
                }
                ChannelEvents::Status(val) => serializer.serialize_newtype_variant(name, 1, "Status", val),
            }
        }
    }

    struct EventsBoxVisitor;

    impl<'de> Visitor<'de> for EventsBoxVisitor {
        type Value = Box<dyn Events>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "Events object")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            use items_0::subfr::SubFrId;
            let e0: &str = seq.next_element()?.ok_or(de::Error::missing_field("ty .0"))?;
            let e1: u32 = seq.next_element()?.ok_or(de::Error::missing_field("nty .1"))?;
            if e0 == EventsDim0::<u8>::serde_id() {
                match e1 {
                    i32::SUB => {
                        let obj: EventsDim0<i32> = seq.next_element()?.ok_or(de::Error::missing_field("obj .2"))?;
                        Ok(Box::new(obj))
                    }
                    f32::SUB => {
                        let obj: EventsDim0<f32> = seq.next_element()?.ok_or(de::Error::missing_field("obj .2"))?;
                        Ok(Box::new(obj))
                    }
                    _ => Err(de::Error::custom(&format!("unknown nty {e1}"))),
                }
            } else {
                Err(de::Error::custom(&format!("unknown ty {e0}")))
            }
        }
    }

    pub struct ChannelEventsVisitor;

    impl ChannelEventsVisitor {
        fn name() -> &'static str {
            "ChannelEvents"
        }

        fn allowed_variants() -> &'static [&'static str] {
            &["Events", "Status", "RangeComplete"]
        }
    }

    impl<'de> Visitor<'de> for ChannelEventsVisitor {
        type Value = ChannelEvents;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "ChannelEvents")
        }

        fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
        where
            A: EnumAccess<'de>,
        {
            let (id, var) = data.variant()?;
            match id {
                "Events" => {
                    let c = var.tuple_variant(3, EventsBoxVisitor)?;
                    Ok(Self::Value::Events(c))
                }
                _ => return Err(de::Error::unknown_variant(id, Self::allowed_variants())),
            }
        }
    }

    impl<'de> Deserialize<'de> for ChannelEvents {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            de.deserialize_enum(
                ChannelEventsVisitor::name(),
                ChannelEventsVisitor::allowed_variants(),
                ChannelEventsVisitor,
            )
        }
    }
}

#[cfg(test)]
mod test_channel_events_serde {
    use super::ChannelEvents;
    use crate::eventsdim0::EventsDim0;
    use items_0::Empty;

    #[test]
    fn channel_events() {
        let mut evs = EventsDim0::empty();
        evs.push(8, 2, 3.0f32);
        evs.push(12, 3, 3.2f32);
        let item = ChannelEvents::Events(Box::new(evs));
        let s = serde_json::to_string_pretty(&item).unwrap();
        eprintln!("{s}");
        let w: ChannelEvents = serde_json::from_str(&s).unwrap();
        eprintln!("{w:?}");
    }
}

impl PartialEq for ChannelEvents {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Events(l0), Self::Events(r0)) => l0 == r0,
            (Self::Status(l0), Self::Status(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl MergeableCev for ChannelEvents {
    fn ts_min(&self) -> Option<u64> {
        use ChannelEvents::*;
        match self {
            Events(k) => k.ts_min(),
            Status(k) => Some(k.ts),
        }
    }

    fn ts_max(&self) -> Option<u64> {
        error!("TODO impl MergableEvents for ChannelEvents");
        err::todoval()
    }
}

impl crate::merger::Mergeable for ChannelEvents {
    fn len(&self) -> usize {
        match self {
            ChannelEvents::Events(k) => k.len(),
            ChannelEvents::Status(_) => 1,
        }
    }

    fn ts_min(&self) -> Option<u64> {
        match self {
            ChannelEvents::Events(k) => k.ts_min(),
            ChannelEvents::Status(k) => Some(k.ts),
        }
    }

    fn ts_max(&self) -> Option<u64> {
        match self {
            ChannelEvents::Events(k) => k.ts_max(),
            ChannelEvents::Status(k) => Some(k.ts),
        }
    }

    fn is_compatible_target(&self, tgt: &Self) -> bool {
        use ChannelEvents::*;
        match self {
            Events(_) => {
                // TODO better to delegate this to inner type?
                if let Events(_) = tgt {
                    true
                } else {
                    false
                }
            }
            Status(_) => {
                // TODO better to delegate this to inner type?
                if let Status(_) = tgt {
                    true
                } else {
                    false
                }
            }
        }
    }

    fn move_into_fresh(&mut self, ts_end: u64) -> Self {
        match self {
            ChannelEvents::Events(k) => ChannelEvents::Events(k.move_into_fresh(ts_end)),
            ChannelEvents::Status(k) => ChannelEvents::Status(k.clone()),
        }
    }

    fn move_into_existing(&mut self, tgt: &mut Self, ts_end: u64) -> Result<(), merger::MergeError> {
        match self {
            ChannelEvents::Events(k) => match tgt {
                ChannelEvents::Events(tgt) => k.move_into_existing(tgt, ts_end),
                ChannelEvents::Status(_) => Err(merger::MergeError::NotCompatible),
            },
            ChannelEvents::Status(_) => match tgt {
                ChannelEvents::Events(_) => Err(merger::MergeError::NotCompatible),
                ChannelEvents::Status(_) => Err(merger::MergeError::Full),
            },
        }
    }
}

impl Collectable for ChannelEvents {
    fn new_collector(&self) -> Box<dyn Collector> {
        match self {
            ChannelEvents::Events(_item) => todo!(),
            ChannelEvents::Status(_) => todo!(),
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct ChannelEventsTimeBinner {
    // TODO `ConnStatus` contains all the changes that can happen to a connection, but
    // here we would rather require a simplified current state for binning purpose.
    edges: Vec<u64>,
    do_time_weight: bool,
    conn_state: ConnStatus,
    binner: Option<Box<dyn crate::TimeBinner>>,
}

impl fmt::Debug for ChannelEventsTimeBinner {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ChannelEventsTimeBinner")
            .field("conn_state", &self.conn_state)
            .finish()
    }
}

impl crate::timebin::TimeBinner for ChannelEventsTimeBinner {
    type Input = ChannelEvents;
    type Output = Box<dyn items_0::TimeBinned>;

    fn ingest(&mut self, item: &mut Self::Input) {
        match item {
            ChannelEvents::Events(item) => {
                if self.binner.is_none() {
                    let binner = item.time_binner_new(self.edges.clone(), self.do_time_weight);
                    self.binner = Some(binner);
                }
                match self.binner.as_mut() {
                    Some(binner) => binner.ingest(item.as_time_binnable()),
                    None => {
                        error!("ingest without active binner item {item:?}");
                        ()
                    }
                }
            }
            ChannelEvents::Status(item) => {
                warn!("TODO consider channel status in time binning {item:?}");
            }
        }
    }

    fn set_range_complete(&mut self) {
        match self.binner.as_mut() {
            Some(binner) => binner.set_range_complete(),
            None => (),
        }
    }

    fn bins_ready_count(&self) -> usize {
        match &self.binner {
            Some(binner) => binner.bins_ready_count(),
            None => 0,
        }
    }

    fn bins_ready(&mut self) -> Option<Self::Output> {
        match self.binner.as_mut() {
            Some(binner) => binner.bins_ready(),
            None => None,
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        match self.binner.as_mut() {
            Some(binner) => binner.push_in_progress(push_empty),
            None => (),
        }
    }

    fn cycle(&mut self) {
        match self.binner.as_mut() {
            Some(binner) => binner.cycle(),
            None => (),
        }
    }
}

impl crate::timebin::TimeBinnable for ChannelEvents {
    type TimeBinner = ChannelEventsTimeBinner;

    fn time_binner_new(&self, edges: Vec<u64>, do_time_weight: bool) -> Self::TimeBinner {
        let (binner, status) = match self {
            ChannelEvents::Events(_events) => (None, ConnStatus::Connect),
            ChannelEvents::Status(status) => (None, status.status.clone()),
        };
        ChannelEventsTimeBinner {
            edges,
            do_time_weight,
            conn_state: status,
            binner,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelEventsCollectorOutput {}

impl items_0::AsAnyRef for ChannelEventsCollectorOutput {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl crate::ToJsonResult for ChannelEventsCollectorOutput {
    fn to_json_result(&self) -> Result<Box<dyn items_0::collect_s::ToJsonBytes>, err::Error> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl items_0::collect_c::Collected for ChannelEventsCollectorOutput {}

#[derive(Debug)]
pub struct ChannelEventsCollector {
    coll: Option<Box<dyn items_0::collect_c::CollectorDyn>>,
    range_complete: bool,
    timed_out: bool,
}

impl ChannelEventsCollector {
    pub fn new() -> Self {
        Self {
            coll: None,
            range_complete: false,
            timed_out: false,
        }
    }
}

impl items_0::collect_c::Collector for ChannelEventsCollector {
    type Input = ChannelEvents;
    type Output = Box<dyn items_0::collect_c::Collected>;

    fn len(&self) -> usize {
        match &self.coll {
            Some(coll) => coll.len(),
            None => 0,
        }
    }

    fn ingest(&mut self, item: &mut Self::Input) {
        match item {
            ChannelEvents::Events(item) => {
                if self.coll.is_none() {
                    let coll = item.as_ref().as_collectable_with_default_ref().new_collector();
                    self.coll = Some(coll);
                }
                let coll = self.coll.as_mut().unwrap();
                coll.ingest(item.as_collectable_with_default_mut());
            }
            ChannelEvents::Status(_) => {
                // TODO decide on output format to collect also the connection status events
            }
        }
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self) -> Result<Self::Output, err::Error> {
        match self.coll.as_mut() {
            Some(coll) => {
                if self.range_complete {
                    coll.set_range_complete();
                }
                if self.timed_out {
                    coll.set_timed_out();
                }
                let res = coll.result()?;
                //error!("fix output of ChannelEventsCollector [03ce6bc5a]");
                //err::todo();
                //let output = ChannelEventsCollectorOutput {};
                Ok(res)
            }
            None => {
                error!("nothing collected [caa8d2565]");
                todo!()
            }
        }
    }
}

impl items_0::collect_c::Collectable for ChannelEvents {
    type Collector = ChannelEventsCollector;

    fn new_collector(&self) -> Self::Collector {
        ChannelEventsCollector::new()
    }
}
