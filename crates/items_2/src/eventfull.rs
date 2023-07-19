use crate::framable::FrameType;
use crate::merger::Mergeable;
use bitshuffle::bitshuffle_decompress;
use bytes::BytesMut;
use err::thiserror;
use err::ThisError;
use items_0::container::ByteEstimate;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::EVENT_FULL_FRAME_TYPE_ID;
use items_0::Empty;
use items_0::MergeError;
use items_0::WithLen;
#[allow(unused)]
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use parse::channelconfig::CompressionMethod;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::time::Instant;

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*) };
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventFull {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub blobs: VecDeque<Vec<u8>>,
    //#[serde(with = "decomps_serde")]
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
    pub fn push(
        &mut self,
        ts: u64,
        pulse: u64,
        blob: Vec<u8>,
        scalar_type: ScalarType,
        be: bool,
        shape: Shape,
        comp: Option<CompressionMethod>,
    ) {
        let m1 = blob.len();
        self.entry_payload_max = self.entry_payload_max.max(m1 as u64);
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.blobs.push_back(blob);
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
        self.scalar_types.truncate(nkeep);
        self.be.truncate(nkeep);
        self.shapes.truncate(nkeep);
        self.comps.truncate(nkeep);
    }

    // NOTE needed because the databuffer actually doesn't write the correct shape per event.
    pub fn overwrite_all_shapes(&mut self, shape: &Shape) {
        for u in &mut self.shapes {
            *u = shape.clone();
        }
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
            max = max.max(self.blobs[i].len() as _);
        }
        dst.entry_payload_max = max;
        dst.tss.extend(self.tss.drain(r.clone()));
        dst.pulses.extend(self.pulses.drain(r.clone()));
        dst.blobs.extend(self.blobs.drain(r.clone()));
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

#[derive(Debug, ThisError, Serialize, Deserialize)]
pub enum DecompError {
    TooLittleInput,
    BadCompresionBlockSize,
    UnusedBytes,
    BitshuffleError,
}

fn decompress(databuf: &[u8], type_size: u32, ele_count_2: u64, ele_count_exp: u64) -> Result<Vec<u8>, DecompError> {
    let ts1 = Instant::now();
    if databuf.len() < 12 {
        return Err(DecompError::TooLittleInput);
    }
    let value_bytes = u64::from_be_bytes(databuf[0..8].try_into().unwrap());
    let block_size = u32::from_be_bytes(databuf[8..12].try_into().unwrap());
    trace2!(
        "decompress  len {}  value_bytes {}  block_size {}",
        databuf.len(),
        value_bytes,
        block_size
    );
    if block_size > 1024 * 32 {
        return Err(DecompError::BadCompresionBlockSize);
    }
    let ele_count = value_bytes / type_size as u64;
    trace2!(
        "ele_count {}  ele_count_2 {}  ele_count_exp {}",
        ele_count,
        ele_count_2,
        ele_count_exp
    );
    let mut decomp = Vec::with_capacity(type_size as usize * ele_count as usize);
    unsafe {
        decomp.set_len(decomp.capacity());
    }
    match bitshuffle_decompress(&databuf[12..], &mut decomp, ele_count as _, type_size as _, 0) {
        Ok(c1) => {
            if 12 + c1 != databuf.len() {
                Err(DecompError::UnusedBytes)
            } else {
                let ts2 = Instant::now();
                let _dt = ts2.duration_since(ts1);
                // TODO analyze the histo
                //self.decomp_dt_histo.ingest(dt.as_secs() as u32 + dt.subsec_micros());
                Ok(decomp)
            }
        }
        Err(_) => Err(DecompError::BitshuffleError),
    }
}

impl EventFull {
    pub fn data_raw(&self, i: usize) -> &[u8] {
        &self.blobs[i]
    }

    pub fn data_decompressed(
        &self,
        i: usize,
        _scalar_type: &ScalarType,
        shape: &Shape,
    ) -> Result<Cow<[u8]>, DecompError> {
        if let Some(comp) = &self.comps[i] {
            match comp {
                CompressionMethod::BitshuffleLZ4 => {
                    let type_size = self.scalar_types[i].bytes() as u32;
                    let ele_count = self.shapes[i].ele_count();
                    let data = decompress(&self.blobs[i], type_size, ele_count, shape.ele_count())?;
                    Ok(Cow::Owned(data))
                }
            }
        } else {
            let data = &self.blobs[i];
            Ok(Cow::Borrowed(data.as_slice()))
        }
    }
}
