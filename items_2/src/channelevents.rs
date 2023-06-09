use crate::merger;
use crate::merger_cev::MergeableCev;
use crate::Events;
use items::FrameType;
use items::FrameTypeInnerStatic;
use items_0::collect_s::Collectable;
use items_0::collect_s::Collector;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::NanoRange;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::fmt;
use std::time::Duration;
use std::time::SystemTime;

// TODO maybe rename to ChannelStatus?
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConnStatus {
    Connect,
    Disconnect,
}

impl ConnStatus {
    pub fn from_ca_ingest_status_kind(k: u32) -> Self {
        match k {
            1 => Self::Connect,
            _ => Self::Disconnect,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnStatusEvent {
    pub ts: u64,
    #[serde(with = "humantime_serde")]
    //pub datetime: chrono::DateTime<chrono::Utc>,
    pub datetime: SystemTime,
    pub status: ConnStatus,
}

impl ConnStatusEvent {
    pub fn new(ts: u64, status: ConnStatus) -> Self {
        let datetime = SystemTime::UNIX_EPOCH + Duration::from_millis(ts / 1000000);
        Self { ts, datetime, status }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChannelStatus {
    Connect,
    Disconnect,
}

impl ChannelStatus {
    pub fn from_ca_ingest_status_kind(k: u32) -> Self {
        match k {
            1 => Self::Connect,
            _ => Self::Disconnect,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelStatusEvent {
    pub ts: u64,
    #[serde(with = "humantime_serde")]
    //pub datetime: chrono::DateTime<chrono::Utc>,
    pub datetime: SystemTime,
    pub status: ChannelStatus,
}

impl ChannelStatusEvent {
    pub fn new(ts: u64, status: ChannelStatus) -> Self {
        let datetime = SystemTime::UNIX_EPOCH + Duration::from_millis(ts / 1000000);
        Self { ts, datetime, status }
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

impl AsAnyRef for ChannelEvents {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl AsAnyMut for ChannelEvents {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

mod serde_channel_events {
    use super::{ChannelEvents, Events};
    use crate::channelevents::ConnStatusEvent;
    use crate::eventsdim0::EventsDim0;
    use crate::eventsdim1::EventsDim1;
    use items_0::subfr::SubFrId;
    use serde::de::{self, EnumAccess, VariantAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    struct EvRef<'a>(&'a dyn Events);

    struct EvBox(Box<dyn Events>);

    impl<'a> Serialize for EvRef<'a> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut ser = serializer.serialize_seq(Some(3))?;
            ser.serialize_element(self.0.serde_id())?;
            ser.serialize_element(&self.0.nty_id())?;
            ser.serialize_element(self.0)?;
            ser.end()
        }
    }

    struct EvBoxVis;

    impl EvBoxVis {
        fn name() -> &'static str {
            "Events"
        }
    }

    impl<'de> Visitor<'de> for EvBoxVis {
        type Value = EvBox;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "{}", Self::name())
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let e0: &str = seq.next_element()?.ok_or(de::Error::missing_field("[0] cty"))?;
            let e1: u32 = seq.next_element()?.ok_or(de::Error::missing_field("[1] nty"))?;
            if e0 == EventsDim0::<u8>::serde_id() {
                match e1 {
                    u8::SUB => {
                        let obj: EventsDim0<u8> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u16::SUB => {
                        let obj: EventsDim0<u16> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u32::SUB => {
                        let obj: EventsDim0<u32> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u64::SUB => {
                        let obj: EventsDim0<u64> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i8::SUB => {
                        let obj: EventsDim0<i8> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i16::SUB => {
                        let obj: EventsDim0<i16> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i32::SUB => {
                        let obj: EventsDim0<i32> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i64::SUB => {
                        let obj: EventsDim0<i64> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    f32::SUB => {
                        let obj: EventsDim0<f32> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    f64::SUB => {
                        let obj: EventsDim0<f64> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    bool::SUB => {
                        let obj: EventsDim0<bool> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    _ => Err(de::Error::custom(&format!("unknown nty {e1}"))),
                }
            } else if e0 == EventsDim1::<u8>::serde_id() {
                match e1 {
                    f32::SUB => {
                        let obj: EventsDim1<f32> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    bool::SUB => {
                        let obj: EventsDim1<bool> = seq.next_element()?.ok_or(de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    _ => Err(de::Error::custom(&format!("unknown nty {e1}"))),
                }
            } else {
                Err(de::Error::custom(&format!("unknown cty {e0}")))
            }
        }
    }

    impl<'de> Deserialize<'de> for EvBox {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(EvBoxVis)
        }
    }

    impl Serialize for ChannelEvents {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let name = "ChannelEvents";
            let vars = ChannelEventsVis::allowed_variants();
            match self {
                ChannelEvents::Events(obj) => {
                    serializer.serialize_newtype_variant(name, 0, vars[0], &EvRef(obj.as_ref()))
                }
                ChannelEvents::Status(val) => serializer.serialize_newtype_variant(name, 1, vars[1], val),
            }
        }
    }

    enum VarId {
        Events,
        Status,
    }

    struct VarIdVis;

    impl<'de> Visitor<'de> for VarIdVis {
        type Value = VarId;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "variant identifier")
        }

        fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match val {
                0 => Ok(VarId::Events),
                1 => Ok(VarId::Status),
                _ => Err(de::Error::invalid_value(
                    de::Unexpected::Unsigned(val),
                    &"variant index 0..2",
                )),
            }
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let vars = ChannelEventsVis::allowed_variants();
            if val == vars[0] {
                Ok(VarId::Events)
            } else if val == vars[1] {
                Ok(VarId::Status)
            } else {
                Err(de::Error::unknown_variant(val, ChannelEventsVis::allowed_variants()))
            }
        }
    }

    impl<'de> Deserialize<'de> for VarId {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            de.deserialize_identifier(VarIdVis)
        }
    }

    pub struct ChannelEventsVis;

    impl ChannelEventsVis {
        fn name() -> &'static str {
            "ChannelEvents"
        }

        fn allowed_variants() -> &'static [&'static str] {
            &["Events", "Status"]
        }
    }

    impl<'de> Visitor<'de> for ChannelEventsVis {
        type Value = ChannelEvents;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "{}", Self::name())
        }

        fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
        where
            A: EnumAccess<'de>,
        {
            let (id, var) = data.variant()?;
            match id {
                VarId::Events => {
                    let x: EvBox = var.newtype_variant()?;
                    Ok(Self::Value::Events(x.0))
                }
                VarId::Status => {
                    let x: ConnStatusEvent = var.newtype_variant()?;
                    Ok(Self::Value::Status(x))
                }
            }
        }
    }

    impl<'de> Deserialize<'de> for ChannelEvents {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            de.deserialize_enum(
                ChannelEventsVis::name(),
                ChannelEventsVis::allowed_variants(),
                ChannelEventsVis,
            )
        }
    }
}

#[cfg(test)]
mod test_channel_events_serde {
    use super::ChannelEvents;
    use crate::channelevents::ConnStatusEvent;
    use crate::eventsdim0::EventsDim0;
    use bincode::config::FixintEncoding;
    use bincode::config::LittleEndian;
    use bincode::config::RejectTrailing;
    use bincode::config::WithOtherEndian;
    use bincode::config::WithOtherIntEncoding;
    use bincode::config::WithOtherTrailing;
    use bincode::DefaultOptions;
    use items_0::bincode;
    use items_0::Empty;
    use serde::Deserialize;
    use serde::Serialize;
    use std::time::SystemTime;

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

    type OptsTy = WithOtherTrailing<
        WithOtherIntEncoding<WithOtherEndian<DefaultOptions, LittleEndian>, FixintEncoding>,
        RejectTrailing,
    >;

    fn bincode_opts() -> OptsTy {
        use bincode::Options;
        let opts = bincode::DefaultOptions::new()
            .with_little_endian()
            .with_fixint_encoding()
            .reject_trailing_bytes();
        opts
    }

    #[test]
    fn channel_events_bincode() {
        let mut evs = EventsDim0::empty();
        evs.push(8, 2, 3.0f32);
        evs.push(12, 3, 3.2f32);
        let item = ChannelEvents::Events(Box::new(evs));
        let opts = bincode_opts();
        let mut out = Vec::new();
        let mut ser = bincode::Serializer::new(&mut out, opts);
        item.serialize(&mut ser).unwrap();
        eprintln!("serialized into {} bytes", out.len());
        let mut de = bincode::Deserializer::from_slice(&out, opts);
        let item = <ChannelEvents as Deserialize>::deserialize(&mut de).unwrap();
        let item = if let ChannelEvents::Events(x) = item {
            x
        } else {
            panic!()
        };
        let item: &EventsDim0<f32> = item.as_any_ref().downcast_ref().unwrap();
        assert_eq!(item.tss().len(), 2);
        assert_eq!(item.tss()[1], 12);
    }

    #[test]
    fn channel_status_bincode() {
        let mut evs = EventsDim0::empty();
        evs.push(8, 2, 3.0f32);
        evs.push(12, 3, 3.2f32);
        let status = ConnStatusEvent {
            ts: 567,
            datetime: SystemTime::UNIX_EPOCH,
            status: crate::channelevents::ConnStatus::Connect,
        };
        let item = ChannelEvents::Status(status);
        let opts = bincode_opts();
        let mut out = Vec::new();
        let mut ser = bincode::Serializer::new(&mut out, opts);
        item.serialize(&mut ser).unwrap();
        eprintln!("serialized into {} bytes", out.len());
        let mut de = bincode::Deserializer::from_slice(&out, opts);
        let item = <ChannelEvents as Deserialize>::deserialize(&mut de).unwrap();
        let item = if let ChannelEvents::Status(x) = item {
            x
        } else {
            panic!()
        };
        assert_eq!(item.ts, 567);
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

    fn empty(&self) -> Option<Self::Output> {
        match self.binner.as_ref() {
            Some(binner) => Some(binner.empty()),
            None => None,
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

impl AsAnyRef for ChannelEventsCollectorOutput {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl AsAnyMut for ChannelEventsCollectorOutput {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl crate::ToJsonResult for ChannelEventsCollectorOutput {
    fn to_json_result(&self) -> Result<Box<dyn items_0::collect_s::ToJsonBytes>, err::Error> {
        todo!()
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
    fn len(&self) -> usize {
        match &self.coll {
            Some(coll) => coll.len(),
            None => 0,
        }
    }

    fn ingest(&mut self, item: &mut dyn items_0::collect_c::Collectable) {
        if let Some(item) = item.as_any_mut().downcast_mut::<ChannelEvents>() {
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
        } else {
            error!("ChannelEventsCollector::ingest unexpected item {:?}", item);
        }
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(
        &mut self,
        range: Option<NanoRange>,
        binrange: Option<BinnedRange>,
    ) -> Result<Box<dyn items_0::collect_c::Collected>, err::Error> {
        match self.coll.as_mut() {
            Some(coll) => {
                if self.range_complete {
                    coll.set_range_complete();
                }
                if self.timed_out {
                    coll.set_timed_out();
                }
                let res = coll.result(range, binrange)?;
                Ok(res)
            }
            None => {
                error!("nothing collected [caa8d2565]");
                Err(err::Error::with_public_msg_no_trace("nothing collected [caa8d2565]"))
            }
        }
    }
}

impl items_0::WithLen for ChannelEvents {
    fn len(&self) -> usize {
        match self {
            ChannelEvents::Events(k) => k.len(),
            ChannelEvents::Status(_) => 1,
        }
    }
}
impl items_0::collect_c::Collectable for ChannelEvents {
    fn new_collector(&self) -> Box<dyn items_0::collect_c::Collector> {
        Box::new(ChannelEventsCollector::new())
    }
}
