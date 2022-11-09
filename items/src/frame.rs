use crate::inmem::InMemoryFrame;
use crate::{FrameDecodable, FrameType, LogItem, StatsItem};
use crate::{
    ERROR_FRAME_TYPE_ID, INMEM_FRAME_ENCID, INMEM_FRAME_HEAD, INMEM_FRAME_MAGIC, LOG_FRAME_TYPE_ID,
    RANGE_COMPLETE_FRAME_TYPE_ID, STATS_FRAME_TYPE_ID, TERM_FRAME_TYPE_ID,
};
use bytes::{BufMut, BytesMut};
use err::Error;
#[allow(unused)]
use netpod::log::*;
use serde::Serialize;

pub fn make_frame<FT>(item: &FT) -> Result<BytesMut, Error>
where
    FT: FrameType + Serialize,
{
    if item.is_err() {
        make_error_frame(item.err().unwrap())
    } else {
        make_frame_2(item, item.frame_type_id())
    }
}

pub fn make_frame_2<FT>(item: &FT, fty: u32) -> Result<BytesMut, Error>
where
    FT: erased_serde::Serialize,
{
    trace!("make_frame_2  fty {:x}", fty);
    let mut out = vec![];
    use bincode::Options;
    let opts = bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    let mut ser = bincode::Serializer::new(&mut out, opts);
    //let mut ser = serde_json::Serializer::new(std::io::stdout());
    let mut ser2 = <dyn erased_serde::Serializer>::erase(&mut ser);
    //match bincode::serialize(item) {
    match item.erased_serialize(&mut ser2) {
        Ok(_) => {
            let enc = out;
            if enc.len() > u32::MAX as usize {
                return Err(Error::with_msg(format!("too long payload {}", enc.len())));
            }
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(enc.len() + INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(fty);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            // TODO add padding to align to 8 bytes.
            //trace!("enc len {}", enc.len());
            //trace!("payload_crc {}", payload_crc);
            buf.put(enc.as_ref());
            let mut h = crc32fast::Hasher::new();
            h.update(&buf);
            let frame_crc = h.finalize();
            buf.put_u32_le(frame_crc);
            //trace!("frame_crc {}", frame_crc);
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

// TODO remove duplication for these similar `make_*_frame` functions:

pub fn make_error_frame(error: &::err::Error) -> Result<BytesMut, Error> {
    match bincode::serialize(error) {
        Ok(enc) => {
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(ERROR_FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            // TODO add padding to align to 8 bytes.
            //trace!("enc len {}", enc.len());
            //trace!("payload_crc {}", payload_crc);
            buf.put(enc.as_ref());
            let mut h = crc32fast::Hasher::new();
            h.update(&buf);
            let frame_crc = h.finalize();
            buf.put_u32_le(frame_crc);
            //trace!("frame_crc {}", frame_crc);
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

pub fn make_log_frame(item: &LogItem) -> Result<BytesMut, Error> {
    match bincode::serialize(item) {
        Ok(enc) => {
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(LOG_FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            // TODO add padding to align to 8 bytes.
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

pub fn make_stats_frame(item: &StatsItem) -> Result<BytesMut, Error> {
    match bincode::serialize(item) {
        Ok(enc) => {
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(STATS_FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            // TODO add padding to align to 8 bytes.
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

pub fn make_range_complete_frame() -> Result<BytesMut, Error> {
    let enc = [];
    let mut h = crc32fast::Hasher::new();
    h.update(&enc);
    let payload_crc = h.finalize();
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(INMEM_FRAME_ENCID);
    buf.put_u32_le(RANGE_COMPLETE_FRAME_TYPE_ID);
    buf.put_u32_le(enc.len() as u32);
    buf.put_u32_le(payload_crc);
    // TODO add padding to align to 8 bytes.
    buf.put(enc.as_ref());
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    Ok(buf)
}

pub fn make_term_frame() -> Result<BytesMut, Error> {
    let enc = [];
    let mut h = crc32fast::Hasher::new();
    h.update(&enc);
    let payload_crc = h.finalize();
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(INMEM_FRAME_ENCID);
    buf.put_u32_le(TERM_FRAME_TYPE_ID);
    buf.put_u32_le(enc.len() as u32);
    buf.put_u32_le(payload_crc);
    // TODO add padding to align to 8 bytes.
    buf.put(enc.as_ref());
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    Ok(buf)
}

pub fn decode_frame<T>(frame: &InMemoryFrame) -> Result<T, Error>
where
    T: FrameDecodable,
{
    if frame.encid() != INMEM_FRAME_ENCID {
        return Err(Error::with_msg(format!("unknown encoder id {:?}", frame)));
    }
    if frame.len() as usize != frame.buf().len() {
        return Err(Error::with_msg(format!(
            "buf mismatch  {}  vs  {}  in {:?}",
            frame.len(),
            frame.buf().len(),
            frame
        )));
    }
    if frame.tyid() == ERROR_FRAME_TYPE_ID {
        let k: ::err::Error = match bincode::deserialize(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!(
                    "ERROR bincode::deserialize  len {}  ERROR_FRAME_TYPE_ID",
                    frame.buf().len()
                );
                let n = frame.buf().len().min(128);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_error(k))
    } else if frame.tyid() == LOG_FRAME_TYPE_ID {
        let k: LogItem = match bincode::deserialize(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!(
                    "ERROR bincode::deserialize  len {}  LOG_FRAME_TYPE_ID",
                    frame.buf().len()
                );
                let n = frame.buf().len().min(128);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_log(k))
    } else if frame.tyid() == STATS_FRAME_TYPE_ID {
        let k: StatsItem = match bincode::deserialize(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!(
                    "ERROR bincode::deserialize  len {}  STATS_FRAME_TYPE_ID",
                    frame.buf().len()
                );
                let n = frame.buf().len().min(128);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_stats(k))
    } else if frame.tyid() == RANGE_COMPLETE_FRAME_TYPE_ID {
        // There is currently no content in this variant.
        Ok(T::from_range_complete())
    } else {
        let tyid = T::FRAME_TYPE_ID;
        if frame.tyid() != tyid {
            Err(Error::with_msg(format!(
                "type id mismatch  expect {:x}  found {:x}  {:?}",
                tyid,
                frame.tyid(),
                frame
            )))
        } else {
            match bincode::deserialize(frame.buf()) {
                Ok(item) => Ok(item),
                Err(e) => {
                    error!(
                        "ERROR bincode::deserialize  len {}  tyid {:x}",
                        frame.buf().len(),
                        frame.tyid()
                    );
                    let n = frame.buf().len().min(64);
                    let s = String::from_utf8_lossy(&frame.buf()[..n]);
                    error!("frame.buf as string: {:?}", s);
                    Err(e)?
                }
            }
        }
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
