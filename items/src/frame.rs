use crate::inmem::InMemoryFrame;
use crate::{FrameType, INMEM_FRAME_ENCID, INMEM_FRAME_HEAD, INMEM_FRAME_MAGIC};
use bytes::{BufMut, Bytes, BytesMut};
use err::Error;
use serde::{de::DeserializeOwned, Serialize};

pub trait MakeBytesFrame {
    fn make_bytes_frame(&self) -> Result<Bytes, Error> {
        // TODO only implemented for one type, remove
        err::todoval()
    }
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
            let mut buf = BytesMut::with_capacity(enc.len() + INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
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
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(INMEM_FRAME_ENCID);
    buf.put_u32_le(0x01);
    buf.put_u32_le(0);
    buf.put_u32_le(payload_crc);
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    buf
}

pub fn decode_frame<T>(frame: &InMemoryFrame) -> Result<T, Error>
where
    T: FrameType + DeserializeOwned,
{
    if frame.encid() != INMEM_FRAME_ENCID {
        return Err(Error::with_msg(format!("unknown encoder id {:?}", frame)));
    }
    if frame.tyid() != <T as FrameType>::FRAME_TYPE_ID {
        return Err(Error::with_msg(format!(
            "type id mismatch  expect {:x}  found {:?}",
            <T as FrameType>::FRAME_TYPE_ID,
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

pub fn crchex<T>(t: T) -> String
where
    T: AsRef<[u8]>,
{
    let mut h = crc32fast::Hasher::new();
    h.update(t.as_ref());
    let crc = h.finalize();
    format!("{:08x}", crc)
}
