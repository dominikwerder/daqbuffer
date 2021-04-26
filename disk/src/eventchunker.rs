use crate::NeedMinBuffer;
use bitshuffle::bitshuffle_decompress;
use bytes::{Buf, BytesMut};
use err::Error;
use futures_core::Stream;
use futures_util::pin_mut;
use netpod::{ChannelConfig, ScalarType, Shape};
use std::pin::Pin;
use std::task::{Context, Poll};
#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, warn, Level};

pub struct EventChunker {
    inp: NeedMinBuffer,
    polled: u32,
    state: DataFileState,
    need_min: u32,
    channel_config: ChannelConfig,
    errored: bool,
    completed: bool,
}

enum DataFileState {
    FileHeader,
    Event,
}

impl EventChunker {
    pub fn new(
        inp: Pin<Box<dyn Stream<Item = Result<BytesMut, Error>> + Send>>,
        channel_config: ChannelConfig,
    ) -> Self {
        let mut inp = NeedMinBuffer::new(inp);
        inp.set_need_min(6);
        Self {
            inp: inp,
            polled: 0,
            state: DataFileState::FileHeader,
            need_min: 6,
            channel_config,
            errored: false,
            completed: false,
        }
    }

    fn parse_buf(&mut self, buf: &mut BytesMut) -> Result<ParseResult, Error> {
        span!(Level::INFO, "EventChunker::parse_buf").in_scope(|| self.parse_buf_inner(buf))
    }

    fn parse_buf_inner(&mut self, buf: &mut BytesMut) -> Result<ParseResult, Error> {
        // must communicate to caller:
        // what I've found in the buffer
        // what I've consumed from the buffer
        // how many bytes I need min to make progress
        let mut ret = EventFull::empty();
        use byteorder::{ReadBytesExt, BE};
        loop {
            info!("parse_buf  LOOP  buf len {}  need_min {}", buf.len(), self.need_min);
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
                        info!("parse_buf not enough A  totlen {}", totlen);
                        self.need_min = totlen as u32;
                        break;
                    } else {
                        sl.advance(len as usize - 8);
                        let len2 = sl.read_i32::<BE>().unwrap();
                        assert!(len == len2, "len mismatch");
                        let s1 = String::from_utf8(buf.as_ref()[6..(len as usize + 6 - 8)].to_vec()).unwrap();
                        info!("channel name {}  len {}  len2 {}", s1, len, len2);
                        self.state = DataFileState::Event;
                        self.need_min = 4;
                        buf.advance(totlen);
                    }
                }
                DataFileState::Event => {
                    let mut sl = std::io::Cursor::new(buf.as_ref());
                    let len = sl.read_i32::<BE>().unwrap();
                    assert!(len >= 20 && len < 1024 * 1024 * 10);
                    let len = len as u32;
                    info!(
                        "+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-  event len {}",
                        len,
                    );
                    if (buf.len() as u32) < 20 {
                        // TODO gather stats about how often we find not enough input
                        //info!("parse_buf not enough B");
                        self.need_min = len as u32;
                        break;
                    } else if (buf.len() as u32) < len {
                        {
                            // TODO this is just for testing
                            let mut sl = std::io::Cursor::new(buf.as_ref());
                            sl.read_i32::<BE>().unwrap();
                            sl.read_i64::<BE>().unwrap();
                            let ts = sl.read_i64::<BE>().unwrap();
                            info!("parse_buf not enough C   len {}  have {}  ts {}", len, buf.len(), ts);
                        }
                        self.need_min = len as u32;
                        break;
                    } else {
                        let mut sl = std::io::Cursor::new(buf.as_ref());
                        let len1b = sl.read_i32::<BE>().unwrap();
                        assert!(len == len1b as u32);
                        sl.read_i64::<BE>().unwrap();
                        let ts = sl.read_i64::<BE>().unwrap() as u64;
                        let pulse = sl.read_i64::<BE>().unwrap() as u64;
                        sl.read_i64::<BE>().unwrap();
                        let status = sl.read_i8().unwrap();
                        let severity = sl.read_i8().unwrap();
                        let optional = sl.read_i32::<BE>().unwrap();
                        assert!(status == 0);
                        assert!(severity == 0);
                        assert!(optional == -1);
                        let type_flags = sl.read_u8().unwrap();
                        let type_index = sl.read_u8().unwrap();
                        assert!(type_index <= 13);
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
                            let type_size = type_size(type_index);
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
                                    debug!("decompress result  c1 {}  k1 {}", c1, k1);
                                    ret.add_event(ts, pulse, Some(decomp), ScalarType::from_dtype_index(type_index));
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
                        info!("advance and reset need_min");
                        buf.advance(len as usize);
                        self.need_min = 4;
                    }
                }
            }
        }
        info!("AFTER PARSE LOOP  len {}", ret.tss.len());
        Ok(ParseResult { events: ret })
    }
}

fn type_size(ix: u8) -> u32 {
    match ix {
        0 => 1,
        1 => 1,
        2 => 1,
        3 => 1,
        4 => 2,
        5 => 2,
        6 => 2,
        7 => 4,
        8 => 4,
        9 => 8,
        10 => 8,
        11 => 4,
        12 => 8,
        13 => 1,
        _ => panic!("logic"),
    }
}

struct ParseResult {
    events: EventFull,
}

impl Stream for EventChunker {
    type Item = Result<EventFull, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("EventChunker poll_next on completed");
        }
        if self.errored {
            self.completed = true;
            return Ready(None);
        }
        self.polled += 1;
        if self.polled >= 20000 {
            warn!("EventChunker poll limit reached");
            self.errored = true;
            return Poll::Ready(Some(Err(Error::with_msg(format!("EventChunker poll limit reached")))));
        }
        let g = &mut self.inp;
        pin_mut!(g);
        info!("EventChunker  call input poll_next");
        match g.poll_next(cx) {
            Ready(Some(Ok(mut buf))) => {
                info!("EventChunker got buffer  len {}", buf.len());
                let r = self.parse_buf(&mut buf);
                match r {
                    Ok(res) => {
                        if buf.len() > 0 {
                            // TODO gather stats about this:
                            //info!("parse_buf returned {} leftover bytes to me", buf.len());
                            self.inp.put_back(buf);
                        }
                        if self.need_min > 1024 * 8 {
                            let msg = format!("spurious EventChunker asks for need_min {}", self.need_min);
                            warn!("{}", msg);
                            self.errored = true;
                            Ready(Some(Err(Error::with_msg(msg))))
                        } else {
                            let x = self.need_min;
                            self.inp.set_need_min(x);
                            Ready(Some(Ok(res.events)))
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
                self.completed = true;
                Ready(None)
            }
            Pending => Pending,
        }
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
