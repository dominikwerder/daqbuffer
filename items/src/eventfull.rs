use crate::Appendable;
use crate::ByteEstimate;
use crate::Clearable;
use crate::FrameType;
use crate::FrameTypeInnerStatic;
use crate::PushableIndex;
use crate::WithLen;
use crate::WithTimestamps;
use bytes::BytesMut;
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
    pub fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            blobs: VecDeque::new(),
            decomps: VecDeque::new(),
            scalar_types: VecDeque::new(),
            be: VecDeque::new(),
            shapes: VecDeque::new(),
            comps: VecDeque::new(),
        }
    }

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
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.blobs.push_back(blob);
        self.decomps.push_back(decomp);
        self.scalar_types.push_back(scalar_type);
        self.be.push_back(be);
        self.shapes.push_back(shape);
        self.comps.push_back(comp);
    }

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
    const FRAME_TYPE_ID: u32 = crate::EVENT_FULL_FRAME_TYPE_ID;
}

impl FrameType for EventFull {
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

impl WithLen for EventFull {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl Appendable for EventFull {
    fn empty_like_self(&self) -> Self {
        Self::empty()
    }

    // TODO expensive, get rid of it.
    fn append(&mut self, src: &Self) {
        self.tss.extend(&src.tss);
        self.pulses.extend(&src.pulses);
        self.blobs.extend(src.blobs.iter().map(Clone::clone));
        self.decomps.extend(src.decomps.iter().map(Clone::clone));
        self.scalar_types.extend(src.scalar_types.iter().map(Clone::clone));
        self.be.extend(&src.be);
        self.shapes.extend(src.shapes.iter().map(Clone::clone));
        self.comps.extend(src.comps.iter().map(Clone::clone));
    }

    fn append_zero(&mut self, _ts1: u64, _ts2: u64) {
        // TODO do we still need this type?
        todo!()
    }
}

impl Clearable for EventFull {
    fn clear(&mut self) {
        self.tss.clear();
        self.pulses.clear();
        self.blobs.clear();
        self.decomps.clear();
        self.scalar_types.clear();
        self.be.clear();
        self.shapes.clear();
        self.comps.clear();
    }
}

impl WithTimestamps for EventFull {
    fn ts(&self, ix: usize) -> u64 {
        self.tss[ix]
    }
}

impl ByteEstimate for EventFull {
    fn byte_estimate(&self) -> u64 {
        if self.len() == 0 {
            0
        } else {
            // TODO that is clumsy... it assumes homogenous types.
            // TODO improve via a const fn on NTY
            let decomp_len = self.decomps[0].as_ref().map_or(0, |h| h.len());
            self.tss.len() as u64 * (40 + self.blobs[0].as_ref().map_or(0, |x| x.len()) as u64 + decomp_len as u64)
        }
    }
}

impl PushableIndex for EventFull {
    // TODO check all use cases, can't we move?
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push_back(src.tss[ix]);
        self.pulses.push_back(src.pulses[ix]);
        self.blobs.push_back(src.blobs[ix].clone());
        self.decomps.push_back(src.decomps[ix].clone());
        self.scalar_types.push_back(src.scalar_types[ix].clone());
        self.be.push_back(src.be[ix]);
        self.shapes.push_back(src.shapes[ix].clone());
        self.comps.push_back(src.comps[ix].clone());
    }
}
