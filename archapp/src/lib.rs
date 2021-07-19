use err::Error;

#[cfg(feature = "devread")]
pub mod generated;
#[cfg(not(feature = "devread"))]
pub mod generated {}
#[cfg(feature = "devread")]
pub mod parse;
#[cfg(not(feature = "devread"))]
pub mod parsestub;
use items::eventvalues::EventValues;
use items::waveevents::WaveEvents;
use items::{WithLen, WithTimestamps};
#[cfg(not(feature = "devread"))]
pub use parsestub as parse;

pub mod events;
#[cfg(feature = "devread")]
#[cfg(test)]
pub mod test;

fn unescape_archapp_msg(inp: &[u8]) -> Result<Vec<u8>, Error> {
    let mut ret = Vec::with_capacity(inp.len() * 5 / 4);
    let mut esc = false;
    for &k in inp.iter() {
        if k == 0x1b {
            esc = true;
        } else if esc {
            if k == 0x1 {
                ret.push(0x1b);
            } else if k == 0x2 {
                ret.push(0xa);
            } else if k == 0x3 {
                ret.push(0xd);
            } else {
                return Err(Error::with_msg("malformed escaped archapp message"));
            }
            esc = false;
        } else {
            ret.push(k);
        }
    }
    Ok(ret)
}

#[derive(Debug)]
pub enum EventsItem {
    ScalarByte(EventValues<i8>),
    ScalarShort(EventValues<i16>),
    ScalarInt(EventValues<i32>),
    ScalarFloat(EventValues<f32>),
    ScalarDouble(EventValues<f64>),
    WaveByte(WaveEvents<i8>),
    WaveShort(WaveEvents<i16>),
    WaveInt(WaveEvents<i32>),
    WaveFloat(WaveEvents<f32>),
    WaveDouble(WaveEvents<f64>),
}

impl EventsItem {
    pub fn is_wave(&self) -> bool {
        use EventsItem::*;
        match self {
            WaveByte(_) => true,
            WaveShort(_) => true,
            WaveInt(_) => true,
            WaveFloat(_) => true,
            WaveDouble(_) => true,
            _ => false,
        }
    }

    pub fn variant_name(&self) -> String {
        use EventsItem::*;
        match self {
            ScalarByte(item) => format!("ScalarByte"),
            ScalarShort(item) => format!("ScalarShort"),
            ScalarInt(item) => format!("ScalarInt"),
            ScalarFloat(item) => format!("ScalarFloat"),
            ScalarDouble(item) => format!("ScalarDouble"),
            WaveByte(item) => format!("WaveByte({})", item.len()),
            WaveShort(item) => format!("WaveShort({})", item.len()),
            WaveInt(item) => format!("WaveInt({})", item.len()),
            WaveFloat(item) => format!("WaveFloat({})", item.len()),
            WaveDouble(item) => format!("WaveDouble({})", item.len()),
        }
    }
}

impl WithLen for EventsItem {
    fn len(&self) -> usize {
        use EventsItem::*;
        match self {
            ScalarByte(j) => j.len(),
            ScalarShort(j) => j.len(),
            ScalarInt(j) => j.len(),
            ScalarFloat(j) => j.len(),
            ScalarDouble(j) => j.len(),
            WaveByte(j) => j.len(),
            WaveShort(j) => j.len(),
            WaveInt(j) => j.len(),
            WaveFloat(j) => j.len(),
            WaveDouble(j) => j.len(),
        }
    }
}

impl WithTimestamps for EventsItem {
    fn ts(&self, ix: usize) -> u64 {
        use EventsItem::*;
        match self {
            ScalarByte(j) => j.ts(ix),
            ScalarShort(j) => j.ts(ix),
            ScalarInt(j) => j.ts(ix),
            ScalarFloat(j) => j.ts(ix),
            ScalarDouble(j) => j.ts(ix),
            WaveByte(j) => j.ts(ix),
            WaveShort(j) => j.ts(ix),
            WaveInt(j) => j.ts(ix),
            WaveFloat(j) => j.ts(ix),
            WaveDouble(j) => j.ts(ix),
        }
    }
}
