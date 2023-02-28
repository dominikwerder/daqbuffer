use crate::framable::FrameDecodable;
use crate::framable::FrameType;
use crate::framable::INMEM_FRAME_ENCID;
use crate::framable::INMEM_FRAME_HEAD;
use crate::framable::INMEM_FRAME_MAGIC;
use crate::inmem::InMemoryFrame;
use bincode::config::FixintEncoding;
use bincode::config::LittleEndian;
use bincode::config::RejectTrailing;
use bincode::config::WithOtherEndian;
use bincode::config::WithOtherIntEncoding;
use bincode::config::WithOtherTrailing;
use bincode::DefaultOptions;
use bytes::BufMut;
use bytes::BytesMut;
use err::Error;
use items_0::bincode;
use items_0::streamitem::ContainsError;
use items_0::streamitem::LogItem;
use items_0::streamitem::StatsItem;
use items_0::streamitem::ERROR_FRAME_TYPE_ID;
use items_0::streamitem::LOG_FRAME_TYPE_ID;
use items_0::streamitem::RANGE_COMPLETE_FRAME_TYPE_ID;
use items_0::streamitem::STATS_FRAME_TYPE_ID;
use items_0::streamitem::TERM_FRAME_TYPE_ID;
#[allow(unused)]
use netpod::log::*;
use serde::Serialize;

trait EC {
    fn ec(self) -> err::Error;
}

impl EC for rmp_serde::encode::Error {
    fn ec(self) -> err::Error {
        err::Error::with_msg_no_trace(format!("{self:?}"))
    }
}

impl EC for rmp_serde::decode::Error {
    fn ec(self) -> err::Error {
        err::Error::with_msg_no_trace(format!("{self:?}"))
    }
}

pub fn make_frame<FT>(item: &FT) -> Result<BytesMut, Error>
where
    FT: FrameType + ContainsError + Serialize,
{
    if item.is_err() {
        make_error_frame(item.err().unwrap())
    } else {
        make_frame_2(item, item.frame_type_id())
    }
}

pub fn bincode_ser<W>(
    w: W,
) -> bincode::Serializer<
    W,
    WithOtherTrailing<
        WithOtherIntEncoding<WithOtherEndian<DefaultOptions, LittleEndian>, FixintEncoding>,
        RejectTrailing,
    >,
>
where
    W: std::io::Write,
{
    use bincode::Options;
    let opts = DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();
    let ser = bincode::Serializer::new(w, opts);
    ser
}

pub fn bincode_to_vec<S>(item: S) -> Result<Vec<u8>, Error>
where
    S: Serialize,
{
    let mut out = Vec::new();
    let mut ser = bincode_ser(&mut out);
    item.serialize(&mut ser).map_err(|e| format!("{e}"))?;
    Ok(out)
}

pub fn bincode_from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    use bincode::Options;
    let opts = DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();
    let mut de = bincode::Deserializer::from_slice(buf, opts);
    <T as serde::Deserialize>::deserialize(&mut de).map_err(|e| format!("{e}").into())
}

pub fn encode_to_vec<S>(item: S) -> Result<Vec<u8>, Error>
where
    S: Serialize,
{
    if false {
        serde_json::to_vec(&item).map_err(|e| e.into())
    } else {
        bincode_to_vec(&item)
    }
}

pub fn decode_from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    if false {
        serde_json::from_slice(buf).map_err(|e| e.into())
    } else {
        bincode_from_slice(buf)
    }
}

pub fn make_frame_2<T>(item: &T, fty: u32) -> Result<BytesMut, Error>
where
    T: erased_serde::Serialize,
{
    let mut out = Vec::new();
    //let mut ser = rmp_serde::Serializer::new(&mut out).with_struct_map();
    //let writer = ciborium::ser::into_writer(&item, &mut out).unwrap();
    let mut ser = bincode_ser(&mut out);
    let mut ser2 = <dyn erased_serde::Serializer>::erase(&mut ser);
    //let mut ser = serde_json::Serializer::new(&mut out);
    //let mut ser2 = <dyn erased_serde::Serializer>::erase(&mut ser);
    match item.erased_serialize(&mut ser2) {
        Ok(_) => {
            let enc = out;
            if enc.len() > u32::MAX as usize {
                return Err(Error::with_msg(format!("too long payload {}", enc.len())));
            }
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            // TODO reserve also for footer via constant
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
    match encode_to_vec(error) {
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

// TODO can I remove this usage?
pub fn make_log_frame(item: &LogItem) -> Result<BytesMut, Error> {
    warn!("make_log_frame {item:?}");
    match encode_to_vec(item) {
        Ok(enc) => {
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD);
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(LOG_FRAME_TYPE_ID);
            warn!("make_log_frame  payload len {}", enc.len());
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
    match encode_to_vec(item) {
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
        let k: ::err::Error = match decode_from_slice(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!("ERROR deserialize  len {}  ERROR_FRAME_TYPE_ID", frame.buf().len());
                let n = frame.buf().len().min(128);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_error(k))
    } else if frame.tyid() == LOG_FRAME_TYPE_ID {
        let k: LogItem = match decode_from_slice(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!("ERROR deserialize  len {}  LOG_FRAME_TYPE_ID", frame.buf().len());
                let n = frame.buf().len().min(128);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_log(k))
    } else if frame.tyid() == STATS_FRAME_TYPE_ID {
        let k: StatsItem = match decode_from_slice(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!("ERROR deserialize  len {}  STATS_FRAME_TYPE_ID", frame.buf().len());
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
            match decode_from_slice(frame.buf()) {
                Ok(item) => Ok(item),
                Err(e) => {
                    error!("decode_frame  T = {}", std::any::type_name::<T>());
                    error!("ERROR deserialize  len {}  tyid {:x}", frame.buf().len(), frame.tyid());
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
