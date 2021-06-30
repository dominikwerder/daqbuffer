use err::Error;
use serde::Serialize;

#[cfg(feature = "devread")]
pub mod generated;
#[cfg(not(feature = "devread"))]
pub mod generated {}
#[cfg(feature = "devread")]
pub mod parse;
#[cfg(not(feature = "devread"))]
pub mod parsestub;
#[cfg(not(feature = "devread"))]
pub use parsestub as parse;
#[cfg(feature = "devread")]
#[cfg(test)]
pub mod test;

pub trait ItemSer {
    fn serialize(&self) -> Result<Vec<u8>, Error>;
}

impl<T> ItemSer for T
where
    T: Serialize,
{
    fn serialize(&self) -> Result<Vec<u8>, Error> {
        let u = serde_json::to_vec(self)?;
        Ok(u)
    }
}

pub fn unescape_archapp_msg(inp: &[u8]) -> Result<Vec<u8>, Error> {
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
