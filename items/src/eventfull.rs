use bytes::BytesMut;
use netpod::{ScalarType, Shape};
use parse::channelconfig::CompressionMethod;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    Appendable, ByteEstimate, Clearable, FrameType, FrameTypeInnerStatic, PushableIndex, WithLen, WithTimestamps,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct EventFull {
    pub tss: Vec<u64>,
    pub pulses: Vec<u64>,
    pub blobs: Vec<Vec<u8>>,
    #[serde(serialize_with = "decomps_ser", deserialize_with = "decomps_de")]
    // TODO allow access to `decomps` via method which checks first if `blobs` is already the decomp.
    pub decomps: Vec<Option<BytesMut>>,
    pub scalar_types: Vec<ScalarType>,
    pub be: Vec<bool>,
    pub shapes: Vec<Shape>,
    pub comps: Vec<Option<CompressionMethod>>,
}

fn decomps_ser<S>(t: &Vec<Option<BytesMut>>, s: S) -> Result<S::Ok, S::Error>
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

fn decomps_de<'de, D>(d: D) -> Result<Vec<Option<BytesMut>>, D::Error>
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

impl EventFull {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            pulses: vec![],
            blobs: vec![],
            decomps: vec![],
            scalar_types: vec![],
            be: vec![],
            shapes: vec![],
            comps: vec![],
        }
    }

    pub fn add_event(
        &mut self,
        ts: u64,
        pulse: u64,
        blob: Vec<u8>,
        decomp: Option<BytesMut>,
        scalar_type: ScalarType,
        be: bool,
        shape: Shape,
        comp: Option<CompressionMethod>,
    ) {
        self.tss.push(ts);
        self.pulses.push(pulse);
        self.blobs.push(blob);
        self.decomps.push(decomp);
        self.scalar_types.push(scalar_type);
        self.be.push(be);
        self.shapes.push(shape);
        self.comps.push(comp);
    }

    pub fn decomp(&self, i: usize) -> &[u8] {
        match &self.decomps[i] {
            Some(decomp) => &decomp,
            None => &self.blobs[i],
        }
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
        self.tss.extend_from_slice(&src.tss);
        self.pulses.extend_from_slice(&src.pulses);
        self.blobs.extend_from_slice(&src.blobs);
        self.decomps.extend_from_slice(&src.decomps);
        self.scalar_types.extend_from_slice(&src.scalar_types);
        self.be.extend_from_slice(&src.be);
        self.shapes.extend_from_slice(&src.shapes);
        self.comps.extend_from_slice(&src.comps);
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
        if self.tss.len() == 0 {
            0
        } else {
            // TODO that is clumsy... it assumes homogenous types.
            // TODO improve via a const fn on NTY
            let decomp_len = self.decomps[0].as_ref().map_or(0, |h| h.len());
            self.tss.len() as u64 * (40 + self.blobs[0].len() as u64 + decomp_len as u64)
        }
    }
}

impl PushableIndex for EventFull {
    // TODO check all use cases, can't we move?
    fn push_index(&mut self, src: &Self, ix: usize) {
        self.tss.push(src.tss[ix]);
        self.pulses.push(src.pulses[ix]);
        self.blobs.push(src.blobs[ix].clone());
        self.decomps.push(src.decomps[ix].clone());
        self.scalar_types.push(src.scalar_types[ix].clone());
        self.be.push(src.be[ix]);
        self.shapes.push(src.shapes[ix].clone());
        self.comps.push(src.comps[ix].clone());
    }
}
