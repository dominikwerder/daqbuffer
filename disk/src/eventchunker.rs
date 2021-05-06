use crate::{FileChunkRead, NeedMinBuffer};
use bitshuffle::bitshuffle_decompress;
use bytes::{Buf, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
#[allow(unused_imports)]
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{ChannelConfig, EventDataReadStats, NanoRange, ScalarType, Shape};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct EventChunker {
    inp: NeedMinBuffer,
    state: DataFileState,
    need_min: u32,
    channel_config: ChannelConfig,
    errored: bool,
    completed: bool,
    range: NanoRange,
    seen_beyond_range: bool,
    sent_beyond_range: bool,
    data_emit_complete: bool,
    final_stats_sent: bool,
    data_since_last_stats: u32,
    stats_emit_interval: u32,
    parsed_bytes: u64,
}

enum DataFileState {
    FileHeader,
    Event,
}

struct ParseResult {
    events: EventFull,
    parsed_bytes: u64,
}

impl EventChunker {
    pub fn from_start(
        inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>,
        channel_config: ChannelConfig,
        range: NanoRange,
    ) -> Self {
        let mut inp = NeedMinBuffer::new(inp);
        inp.set_need_min(6);
        Self {
            inp,
            state: DataFileState::FileHeader,
            need_min: 6,
            channel_config,
            errored: false,
            completed: false,
            range,
            seen_beyond_range: false,
            sent_beyond_range: false,
            data_emit_complete: false,
            final_stats_sent: false,
            data_since_last_stats: 0,
            stats_emit_interval: 1,
            parsed_bytes: 0,
        }
    }

    pub fn from_event_boundary(
        inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>,
        channel_config: ChannelConfig,
        range: NanoRange,
    ) -> Self {
        let mut inp = NeedMinBuffer::new(inp);
        inp.set_need_min(4);
        Self {
            inp,
            state: DataFileState::Event,
            need_min: 4,
            channel_config,
            errored: false,
            completed: false,
            range,
            seen_beyond_range: false,
            sent_beyond_range: false,
            data_emit_complete: false,
            final_stats_sent: false,
            data_since_last_stats: 0,
            stats_emit_interval: 1,
            parsed_bytes: 0,
        }
    }

    fn parse_buf(&mut self, buf: &mut BytesMut) -> Result<ParseResult, Error> {
        span!(Level::INFO, "EventChunker::parse_buf").in_scope(|| self.parse_buf_inner(buf))
    }

    fn parse_buf_inner(&mut self, buf: &mut BytesMut) -> Result<ParseResult, Error> {
        let mut ret = EventFull::empty();
        let mut parsed_bytes = 0;
        use byteorder::{ReadBytesExt, BE};
        loop {
            trace!("parse_buf  LOOP  buf len {}  need_min {}", buf.len(), self.need_min);
            if (buf.len() as u32) < self.need_min {
                break;
            }
            match self.state {
                DataFileState::FileHeader => {
                    assert!(buf.len() >= 6, "logic");
                    let mut sl = std::io::Cursor::new(buf.as_ref());
                    let fver = sl.read_i16::<BE>().unwrap();
                    assert!(fver == 0, "unexpected file version");
                    let len = sl.read_i32::<BE>().unwrap();
                    assert!(len > 0 && len < 128, "unexpected data file header");
                    let totlen = len as usize + 2;
                    if buf.len() < totlen {
                        debug!("parse_buf not enough A  totlen {}", totlen);
                        self.need_min = totlen as u32;
                        break;
                    } else {
                        sl.advance(len as usize - 8);
                        let len2 = sl.read_i32::<BE>().unwrap();
                        assert!(len == len2, "len mismatch");
                        let s1 = String::from_utf8(buf.as_ref()[6..(len as usize + 6 - 8)].to_vec()).unwrap();
                        info!("channel name {}", s1);
                        self.state = DataFileState::Event;
                        self.need_min = 4;
                        buf.advance(totlen);
                        parsed_bytes += totlen as u64;
                    }
                }
                DataFileState::Event => {
                    let mut sl = std::io::Cursor::new(buf.as_ref());
                    let len = sl.read_i32::<BE>().unwrap();
                    assert!(len >= 20 && len < 1024 * 1024 * 10);
                    let len = len as u32;
                    if (buf.len() as u32) < len {
                        self.need_min = len as u32;
                        break;
                    } else {
                        let mut sl = std::io::Cursor::new(buf.as_ref());
                        let len1b = sl.read_i32::<BE>().unwrap();
                        assert!(len == len1b as u32);
                        let _ttl = sl.read_i64::<BE>().unwrap();
                        let ts = sl.read_i64::<BE>().unwrap() as u64;
                        let pulse = sl.read_i64::<BE>().unwrap() as u64;
                        if ts >= self.range.end {
                            self.seen_beyond_range = true;
                            self.data_emit_complete = true;
                            break;
                        }
                        if ts < self.range.beg {
                            error!("seen before range: {}", ts / SEC);
                        }
                        let _ioc_ts = sl.read_i64::<BE>().unwrap();
                        let status = sl.read_i8().unwrap();
                        let severity = sl.read_i8().unwrap();
                        let optional = sl.read_i32::<BE>().unwrap();
                        assert!(status == 0);
                        assert!(severity == 0);
                        assert!(optional == -1);
                        let type_flags = sl.read_u8().unwrap();
                        let type_index = sl.read_u8().unwrap();
                        assert!(type_index <= 13);
                        let scalar_type = ScalarType::from_dtype_index(type_index)?;
                        use super::dtflags::*;
                        let is_compressed = type_flags & COMPRESSION != 0;
                        let is_array = type_flags & ARRAY != 0;
                        let _is_big_endian = type_flags & BIG_ENDIAN != 0;
                        let is_shaped = type_flags & SHAPE != 0;
                        if let Shape::Wave(_) = self.channel_config.shape {
                            assert!(is_array);
                        }
                        let compression_method = if is_compressed { sl.read_u8().unwrap() } else { 0 };
                        let shape_dim = if is_shaped { sl.read_u8().unwrap() } else { 0 };
                        assert!(compression_method <= 0);
                        assert!(!is_shaped || (shape_dim >= 1 && shape_dim <= 2));
                        let mut shape_lens = [0, 0, 0, 0];
                        for i1 in 0..shape_dim {
                            shape_lens[i1 as usize] = sl.read_u32::<BE>().unwrap();
                        }
                        if is_compressed {
                            //debug!("event  ts {}  is_compressed {}", ts, is_compressed);
                            let value_bytes = sl.read_u64::<BE>().unwrap();
                            let block_size = sl.read_u32::<BE>().unwrap();
                            let p1 = sl.position() as u32;
                            let k1 = len as u32 - p1 - 4;
                            //debug!("event  len {}  ts {}  is_compressed {}  shape_dim {}  len-dim-0 {}  value_bytes {}  block_size {}", len, ts, is_compressed, shape_dim, shape_lens[0], value_bytes, block_size);
                            assert!(value_bytes < 1024 * 256);
                            assert!(block_size < 1024 * 32);
                            //let value_bytes = value_bytes;
                            let type_size = scalar_type.bytes() as u32;
                            let ele_count = value_bytes / type_size as u64;
                            let ele_size = type_size;
                            match self.channel_config.shape {
                                Shape::Wave(dim1count) => {
                                    if dim1count != ele_count as u32 {
                                        Err(Error::with_msg(format!(
                                            "ChannelConfig expects {:?} but event has {:?}",
                                            self.channel_config.shape, ele_count,
                                        )))?;
                                    }
                                }
                                Shape::Scalar => {
                                    if is_array {
                                        Err(Error::with_msg(format!(
                                            "ChannelConfig expects Scalar but we find event is_array"
                                        )))?;
                                    }
                                }
                            }
                            let decomp_bytes = (type_size * ele_count as u32) as usize;
                            let mut decomp = BytesMut::with_capacity(decomp_bytes);
                            unsafe {
                                decomp.set_len(decomp_bytes);
                            }
                            //debug!("try decompress  value_bytes {}  ele_size {}  ele_count {}  type_index {}", value_bytes, ele_size, ele_count, type_index);
                            match bitshuffle_decompress(
                                &buf.as_ref()[p1 as usize..],
                                &mut decomp,
                                ele_count as usize,
                                ele_size as usize,
                                0,
                            ) {
                                Ok(c1) => {
                                    assert!(c1 as u32 == k1);
                                    trace!("decompress result  c1 {}  k1 {}", c1, k1);
                                    if ts < self.range.beg {
                                    } else if ts >= self.range.end {
                                        error!("EVENT AFTER RANGE  {}", ts / SEC);
                                    } else {
                                        ret.add_event(
                                            ts,
                                            pulse,
                                            Some(decomp),
                                            ScalarType::from_dtype_index(type_index)?,
                                        );
                                    }
                                }
                                Err(e) => {
                                    Err(Error::with_msg(format!("decompression failed {:?}", e)))?;
                                }
                            };
                        } else {
                            Err(Error::with_msg(format!(
                                "TODO uncompressed event parsing not yet implemented"
                            )))?;
                        }
                        buf.advance(len as usize);
                        parsed_bytes += len as u64;
                        self.need_min = 4;
                    }
                }
            }
        }
        Ok(ParseResult {
            events: ret,
            parsed_bytes,
        })
    }
}

pub struct EventFull {
    pub tss: Vec<u64>,
    pub pulses: Vec<u64>,
    pub decomps: Vec<Option<BytesMut>>,
    pub scalar_types: Vec<ScalarType>,
}

impl EventFull {
    pub fn empty() -> Self {
        Self {
            tss: vec![],
            pulses: vec![],
            decomps: vec![],
            scalar_types: vec![],
        }
    }

    fn add_event(&mut self, ts: u64, pulse: u64, decomp: Option<BytesMut>, scalar_type: ScalarType) {
        self.tss.push(ts);
        self.pulses.push(pulse);
        self.decomps.push(decomp);
        self.scalar_types.push(scalar_type);
    }
}

pub enum EventChunkerItem {
    Events(EventFull),
    RangeComplete,
    EventDataReadStats(EventDataReadStats),
}

impl Stream for EventChunker {
    type Item = Result<EventChunkerItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("EventChunker poll_next on completed");
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if self.data_since_last_stats >= self.stats_emit_interval {
                self.data_since_last_stats = 0;
                let item = EventDataReadStats {
                    parsed_bytes: self.parsed_bytes,
                };
                self.parsed_bytes = 0;
                let ret = EventChunkerItem::EventDataReadStats(item);
                Ready(Some(Ok(ret)))
            } else if self.sent_beyond_range {
                self.completed = true;
                Ready(None)
            } else if self.final_stats_sent {
                self.sent_beyond_range = true;
                if self.seen_beyond_range {
                    Ready(Some(Ok(EventChunkerItem::RangeComplete)))
                } else {
                    continue 'outer;
                }
            } else if self.data_emit_complete {
                self.data_since_last_stats = 0;
                let item = EventDataReadStats {
                    parsed_bytes: self.parsed_bytes,
                };
                self.parsed_bytes = 0;
                warn!("EMIT FINAL STATS  {:?}", item);
                let ret = EventChunkerItem::EventDataReadStats(item);
                self.final_stats_sent = true;
                Ready(Some(Ok(ret)))
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(mut fcr))) => {
                        let r = self.parse_buf(&mut fcr.buf);
                        match r {
                            Ok(res) => {
                                self.parsed_bytes += res.parsed_bytes;
                                if fcr.buf.len() > 0 {
                                    // TODO gather stats about this:
                                    self.inp.put_back(fcr);
                                }
                                if self.need_min > 1024 * 8 {
                                    let msg = format!("spurious EventChunker asks for need_min {}", self.need_min);
                                    warn!("{}", msg);
                                    self.errored = true;
                                    Ready(Some(Err(Error::with_msg(msg))))
                                } else {
                                    let x = self.need_min;
                                    self.inp.set_need_min(x);
                                    self.data_since_last_stats += 1;
                                    let ret = EventChunkerItem::Events(res.events);
                                    let ret = Ok(ret);
                                    Ready(Some(ret))
                                }
                            }
                            Err(e) => {
                                error!("EventChunker  parse_buf returned error {:?}", e);
                                self.errored = true;
                                Ready(Some(Err(e.into())))
                            }
                        }
                    }
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        self.data_emit_complete = true;
                        continue 'outer;
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
