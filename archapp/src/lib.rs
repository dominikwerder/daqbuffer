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
    ScalarByte(EventValues<i32>),
    ScalarShort(EventValues<i32>),
    ScalarInt(EventValues<i32>),
    ScalarFloat(EventValues<f32>),
    ScalarDouble(EventValues<f64>),
    WaveByte(WaveEvents<i32>),
    WaveShort(WaveEvents<i32>),
    WaveInt(WaveEvents<i32>),
    WaveFloat(WaveEvents<f32>),
    WaveDouble(WaveEvents<f64>),
}
