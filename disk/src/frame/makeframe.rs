use crate::agg::scalarbinbatch::MinMaxAvgScalarBinBatchStreamItem;
use crate::agg::streams::StreamItem;
use crate::binned::BinnedScalarStreamItem;
use crate::cache::pbvfs::PreBinnedItem;
use crate::frame::inmem::InMemoryFrame;
use crate::raw::conn::RawConnOut;
use crate::raw::EventQueryJsonStringFrame;
use bytes::{BufMut, BytesMut};
use err::Error;
use serde::{Deserialize, Serialize};

pub const INMEM_FRAME_HEAD: usize = 20;
pub const INMEM_FRAME_FOOT: usize = 4;
pub const INMEM_FRAME_MAGIC: u32 = 0xc6c3b73d;

pub trait FrameType {
    const FRAME_TYPE_ID: u32;
}

impl FrameType for EventQueryJsonStringFrame {
    const FRAME_TYPE_ID: u32 = 0x03;
}

impl FrameType for RawConnOut {
    const FRAME_TYPE_ID: u32 = 0x04;
}

impl FrameType for Result<StreamItem<BinnedScalarStreamItem>, Error> {
    const FRAME_TYPE_ID: u32 = 0x06;
}

impl FrameType for Result<MinMaxAvgScalarBinBatchStreamItem, Error> {
    const FRAME_TYPE_ID: u32 = 0x07;
}

impl FrameType for Result<StreamItem<PreBinnedItem>, Error> {
    const FRAME_TYPE_ID: u32 = 0x08;
}

pub fn make_frame<FT>(item: &FT) -> Result<BytesMut, Error>
where
    FT: FrameType + Serialize,
{
    match bincode::serialize(item) {
        Ok(enc) => {
            if enc.len() > u32::MAX as usize {
                return Err(Error::with_msg(format!("too long payload {}", enc.len())));
            }
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let encid = 0x12121212;
            let mut buf = BytesMut::with_capacity(enc.len() + INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(encid);
            buf.put_u32_le(FT::FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            buf.put(enc.as_ref());
            let mut h = crc32fast::Hasher::new();
            h.update(&buf);
            let frame_crc = h.finalize();
            buf.put_u32_le(frame_crc);
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

pub fn make_term_frame() -> BytesMut {
    let mut h = crc32fast::Hasher::new();
    h.update(&[]);
    let payload_crc = h.finalize();
    let encid = 0x12121313;
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(encid);
    buf.put_u32_le(0x01);
    buf.put_u32_le(0);
    buf.put_u32_le(payload_crc);
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    buf
}

pub fn decode_frame<'a, FT>(frame: &'a InMemoryFrame) -> Result<FT, Error>
where
    FT: FrameType + Deserialize<'a>,
{
    if frame.encid() != 0x12121212 {
        return Err(Error::with_msg(format!("unknown encoder id {:?}", frame)));
    }
    if frame.tyid() != FT::FRAME_TYPE_ID {
        return Err(Error::with_msg(format!(
            "type id mismatch expect {}  found {:?}",
            FT::FRAME_TYPE_ID,
            frame
        )));
    }
    if frame.len() as usize != frame.buf().len() {
        return Err(Error::with_msg(format!(
            "buf mismatch  {}  vs  {}  in {:?}",
            frame.len(),
            frame.buf().len(),
            frame
        )));
    }
    match bincode::deserialize(frame.buf()) {
        Ok(item) => Ok(item),
        Err(e) => Err(e.into()),
    }
}
