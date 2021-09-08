use crate::generated::EPICSEvent::PayloadType;
use crate::parse::PbFileReader;
use err::Error;
use items::{WithLen, WithTimestamps};

#[derive(Debug)]
pub struct PosTs {
    pub pos: u64,
    pub ts: u64,
}

pub fn parse_all_ts(off: u64, buf: &[u8], payload_type: PayloadType, year: u32) -> Result<Vec<PosTs>, Error> {
    let mut ret = vec![];
    let mut i1 = 0;
    let mut i2 = usize::MAX;
    loop {
        if i1 >= buf.len() {
            break;
        }
        if buf[i1] == 10 {
            if i2 == usize::MAX {
                i2 = i1;
            } else {
                // Have a chunk from i2..i1
                match PbFileReader::parse_buffer(&buf[i2 + 1..i1], payload_type.clone(), year) {
                    Ok(k) => {
                        if k.len() != 1 {
                            return Err(Error::with_msg_no_trace(format!(
                                "parsed buffer contained {} events",
                                k.len()
                            )));
                        } else {
                            let h = PosTs {
                                pos: off + i2 as u64 + 1,
                                ts: k.ts(0),
                            };
                            ret.push(h);
                        }
                    }
                    Err(_e) => {
                        // TODO ignore except if it's the last chunk.
                    }
                }
                i2 = i1;
            }
        }
        i1 += 1;
    }
    Ok(ret)
}
