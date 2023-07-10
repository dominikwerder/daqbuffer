use crate::framable::FrameType;
use crate::merger::Mergeable;
use bytes::BytesMut;
use items_0::container::ByteEstimate;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::EVENT_FULL_FRAME_TYPE_ID;
use items_0::Empty;
use items_0::MergeError;
use items_0::WithLen;
use netpod::ScalarType;
use netpod::Shape;
use parse::channelconfig::CompressionMethod;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use std::collections::VecDeque;

#[derive(Debug, Serialize, Deserialize)]
pub struct EventFull {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub blobs: VecDeque<Option<Vec<u8>>>,
    //#[serde(with = "decomps_serde")]
    // TODO allow access to `decomps` via method which checks first if `blobs` is already the decomp.
    pub decomps: VecDeque<Option<Vec<u8>>>,
    pub scalar_types: VecDeque<ScalarType>,
    pub be: VecDeque<bool>,
    pub shapes: VecDeque<Shape>,
    pub comps: VecDeque<Option<CompressionMethod>>,
    pub entry_payload_max: u64,
}

#[allow(unused)]
mod decomps_serde {
    use super::*;

    pub fn serialize<S>(t: &VecDeque<Option<BytesMut>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let a: Vec<_> = t
            .iter()
            .map(|k| match k {
                None => None,
                Some(j) => Some(j[..].to_vec()),
            })
            .collect();
        Serialize::serialize(&a, s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<VecDeque<Option<BytesMut>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let a: Vec<Option<Vec<u8>>> = Deserialize::deserialize(d)?;
        let a = a
            .iter()
            .map(|k| match k {
                None => None,
                Some(j) => {
                    let mut a = BytesMut::new();
                    a.extend_from_slice(&j);
                    Some(a)
                }
            })
            .collect();
        Ok(a)
    }
}

impl EventFull {
    pub fn add_event(
        &mut self,
        ts: u64,
        pulse: u64,
        blob: Option<Vec<u8>>,
        decomp: Option<Vec<u8>>,
        scalar_type: ScalarType,
        be: bool,
        shape: Shape,
        comp: Option<CompressionMethod>,
    ) {
        let m1 = blob.as_ref().map_or(0, |x| x.len());
        let m2 = decomp.as_ref().map_or(0, |x| x.len());
        self.entry_payload_max = self.entry_payload_max.max(m1 as u64 + m2 as u64);
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.blobs.push_back(blob);
        self.decomps.push_back(decomp);
        self.scalar_types.push_back(scalar_type);
        self.be.push_back(be);
        self.shapes.push_back(shape);
        self.comps.push_back(comp);
    }

    // TODO possible to get rid of this?
    pub fn truncate_ts(&mut self, end: u64) {
        let mut nkeep = usize::MAX;
        for (i, &ts) in self.tss.iter().enumerate() {
            if ts >= end {
                nkeep = i;
                break;
            }
        }
        self.tss.truncate(nkeep);
        self.pulses.truncate(nkeep);
        self.blobs.truncate(nkeep);
        self.decomps.truncate(nkeep);
        self.scalar_types.truncate(nkeep);
        self.be.truncate(nkeep);
        self.shapes.truncate(nkeep);
        self.comps.truncate(nkeep);
    }
}

impl FrameTypeInnerStatic for EventFull {
    const FRAME_TYPE_ID: u32 = EVENT_FULL_FRAME_TYPE_ID;
}

impl FrameType for EventFull {
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

impl Empty for EventFull {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            blobs: VecDeque::new(),
            decomps: VecDeque::new(),
            scalar_types: VecDeque::new(),
            be: VecDeque::new(),
            shapes: VecDeque::new(),
            comps: VecDeque::new(),
            entry_payload_max: 0,
        }
    }
}

impl WithLen for EventFull {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl ByteEstimate for EventFull {
    fn byte_estimate(&self) -> u64 {
        self.len() as u64 * (64 + self.entry_payload_max)
    }
}

impl Mergeable for EventFull {
    fn ts_min(&self) -> Option<u64> {
        self.tss.front().map(|&x| x)
    }

    fn ts_max(&self) -> Option<u64> {
        self.tss.back().map(|&x| x)
    }

    fn new_empty(&self) -> Self {
        Empty::empty()
    }

    fn drain_into(&mut self, dst: &mut Self, range: (usize, usize)) -> Result<(), MergeError> {
        // TODO make it harder to forget new members when the struct may get modified in the future
        let r = range.0..range.1;
        let mut max = dst.entry_payload_max;
        for i in r.clone() {
            let m1 = self.blobs[i].as_ref().map_or(0, |x| x.len());
            let m2 = self.decomps[i].as_ref().map_or(0, |x| x.len());
            max = max.max(m1 as u64 + m2 as u64);
        }
        dst.entry_payload_max = max;
        dst.tss.extend(self.tss.drain(r.clone()));
        dst.pulses.extend(self.pulses.drain(r.clone()));
        dst.blobs.extend(self.blobs.drain(r.clone()));
        dst.decomps.extend(self.decomps.drain(r.clone()));
        dst.scalar_types.extend(self.scalar_types.drain(r.clone()));
        dst.be.extend(self.be.drain(r.clone()));
        dst.shapes.extend(self.shapes.drain(r.clone()));
        dst.comps.extend(self.comps.drain(r.clone()));
        Ok(())
    }

    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m > ts {
                return Some(i);
            }
        }
        None
    }

    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m >= ts {
                return Some(i);
            }
        }
        None
    }

    fn find_highest_index_lt(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate().rev() {
            if m < ts {
                return Some(i);
            }
        }
        None
    }
}
