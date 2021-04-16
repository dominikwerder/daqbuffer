use crate::{BadError, Error};
use nom::number::complete::{be_i16, be_i32, be_i64, be_i8, be_u8};
#[allow(unused_imports)]
use nom::{
    bytes::complete::{tag, take, take_while_m_n},
    combinator::map_res,
    sequence::tuple,
    IResult,
};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::ToPrimitive;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, FromPrimitive, ToPrimitive, Serialize, Deserialize)]
pub enum DType {
    Bool = 0,
    Bool8 = 1,
    Int8 = 2,
    Uint8 = 3,
    Int16 = 4,
    Uint16 = 5,
    Character = 6,
    Int32 = 7,
    Uint32 = 8,
    Int64 = 9,
    Uint64 = 10,
    Float32 = 11,
    Float64 = 12,
    String = 13,
}

impl DType {
    pub fn to_i16(&self) -> i16 {
        ToPrimitive::to_i16(self).unwrap()
    }
}

#[derive(Debug, FromPrimitive, ToPrimitive, Serialize, Deserialize)]
pub enum CompressionMethod {
    BitshuffleLZ4 = 0,
}

impl CompressionMethod {
    pub fn to_i16(&self) -> i16 {
        ToPrimitive::to_i16(self).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub ts: i64,
    pub pulse: i64,
    pub ks: i32,
    pub bs: i64,
    pub splitCount: i32,
    pub status: i32,
    pub bb: i8,
    pub modulo: i32,
    pub offset: i32,
    /*
    Precision:
      0 'default' whatever that is
     -7  f32
    -16  f64
    */
    pub precision: i16,
    pub dtype: DType,
    pub isCompressed: bool,
    pub isShaped: bool,
    pub isArray: bool,
    pub isBigEndian: bool,
    pub compressionMethod: Option<CompressionMethod>,
    pub shape: Option<Vec<u32>>,
    pub sourceName: Option<String>,
    unit: Option<String>,
    description: Option<String>,
    optionalFields: Option<String>,
    valueConverter: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub formatVersion: i16,
    pub channelName: String,
    pub entries: Vec<ConfigEntry>,
}

fn parseShortString(inp: &[u8]) -> Result<(&[u8], Option<String>), Error> {
    let (inp, len1) = be_i32(inp)?;
    if len1 == -1 {
        return Ok((inp, None));
    }
    if len1 < 4 {
        return BadError(format!("bad string len {}", len1));
    }
    if len1 > 500 {
        return BadError(format!("large string len {}", len1));
    }
    let (inp, snb) = take((len1 - 4) as usize)(inp)?;
    let s1 = String::from_utf8(snb.to_vec())?;
    Ok((inp, Some(s1)))
}

/*
Parse a single configuration entry.
*/
pub fn parseEntry(inp: &[u8]) -> Result<(&[u8], Option<ConfigEntry>), Error> {
    let (inp, len1) = be_i32(inp)?;
    if len1 < 0 || len1 > 4000 {
        return BadError(format!("ConfigEntry bad len1 {}", len1));
    }
    if inp.len() == 0 {
        return Ok((inp, None));
    }
    if inp.len() < len1 as usize - 4 {
        return BadError(format!("incomplete input"));
    }
    let inpE = &inp[(len1 - 8) as usize..];
    let (inp, ts) = be_i64(inp)?;
    let (inp, pulse) = be_i64(inp)?;
    let (inp, ks) = be_i32(inp)?;
    let (inp, bs) = be_i64(inp)?;
    let (inp, splitCount) = be_i32(inp)?;
    let (inp, status) = be_i32(inp)?;
    let (inp, bb) = be_i8(inp)?;
    let (inp, modulo) = be_i32(inp)?;
    let (inp, offset) = be_i32(inp)?;
    let (inp, precision) = be_i16(inp)?;
    let (inp, dtlen) = be_i32(inp)?;
    if dtlen > 100 {
        return BadError(format!("unexpected data type len {}", dtlen));
    }
    let (inp, dtmask) = be_u8(inp)?;
    let isCompressed = dtmask & 0x80 != 0;
    let isArray = dtmask & 0x40 != 0;
    let isBigEndian = dtmask & 0x20 != 0;
    let isShaped = dtmask & 0x10 != 0;
    let (inp, dtype) = be_i8(inp)?;
    if dtype > 13 {
        return BadError(format!("unexpected data type {}", dtype));
    }
    let dtype = match num_traits::FromPrimitive::from_i8(dtype) {
        Some(k) => k,
        None => return BadError(format!("Can not convert {} to DType", dtype)),
    };
    let (inp, compressionMethod) = match isCompressed {
        false => (inp, None),
        true => {
            let (inp, cm) = be_u8(inp)?;
            match num_traits::FromPrimitive::from_u8(cm) {
                Some(k) => (inp, Some(k)),
                None => return BadError("unknown compression"),
            }
        }
    };
    let (inp, shape) = match isShaped {
        false => (inp, None),
        true => {
            let (mut inp, dim) = be_u8(inp)?;
            if dim > 4 {
                return BadError(format!("unexpected number of dimensions: {}", dim));
            }
            let mut shape = vec![];
            for _ in 0..dim {
                let t1 = be_i32(inp)?;
                inp = t1.0;
                shape.push(t1.1 as u32);
            }
            (inp, Some(shape))
        }
    };
    let (inp, sourceName) = parseShortString(inp)?;
    let (inp, unit) = parseShortString(inp)?;
    let (inp, description) = parseShortString(inp)?;
    let (inp, optionalFields) = parseShortString(inp)?;
    let (inp, valueConverter) = parseShortString(inp)?;
    assert_eq!(inp.len(), inpE.len());
    let (inpE, len2) = be_i32(inpE)?;
    if len1 != len2 {
        return BadError(format!("mismatch  len1 {}  len2 {}", len1, len2));
    }
    Ok((
        inpE,
        Some(ConfigEntry {
            ts,
            pulse,
            ks,
            bs,
            splitCount,
            status,
            bb,
            modulo,
            offset,
            precision,
            dtype,
            isCompressed,
            isArray,
            isShaped,
            isBigEndian,
            compressionMethod,
            shape,
            sourceName,
            unit,
            description,
            optionalFields,
            valueConverter,
        }),
    ))
}

/*
Parse the full configuration file.
*/
pub fn parseConfig(inp: &[u8]) -> Result<Config, Error> {
    let (inp, ver) = be_i16(inp)?;
    let (inp, len1) = be_i32(inp)?;
    if len1 <= 8 || len1 > 500 {
        return BadError(format!("no channel name.  len1 {}", len1));
    }
    let (inp, chn) = take((len1 - 8) as usize)(inp)?;
    let (inp, len2) = be_i32(inp)?;
    if len1 != len2 {
        return BadError(format!("Mismatch  len1 {}  len2 {}", len1, len2));
    }
    let mut entries = vec![];
    let mut inpA = inp;
    while inpA.len() > 0 {
        let inp = inpA;
        let (inp, e) = parseEntry(inp)?;
        if let Some(e) = e {
            entries.push(e);
        }
        inpA = inp;
    }
    Ok(Config {
        formatVersion: ver,
        channelName: String::from_utf8(chn.to_vec())?,
        entries: entries,
    })
}

#[cfg(test)]
fn read_data() -> Vec<u8> {
    use std::io::Read;
    let mut f1 =
        std::fs::File::open("ks/config/S10CB01-RLOD100-PUP10:SIG-AMPLT/latest/00000_Config")
            .unwrap();
    let mut buf = vec![];
    f1.read_to_end(&mut buf).unwrap();
    buf
}

#[test]
fn parse_dummy() {
    let config =
        parseConfig(&[0, 0, 0, 0, 0, 11, 0x61, 0x62, 0x63, 0, 0, 0, 11, 0, 0, 0, 1]).unwrap();
    assert_eq!(0, config.formatVersion);
    assert_eq!("abc", config.channelName);
}

#[test]
fn open_file() {
    let config = parseConfig(&readData()).unwrap();
    assert_eq!(0, config.formatVersion);
    assert_eq!(9, config.entries.len());
    for e in &config.entries {
        assert!(e.ts >= 631152000000000000);
        assert!(e.ts <= 1591106812800073974);
        assert!(e.shape.is_some());
    }
}
