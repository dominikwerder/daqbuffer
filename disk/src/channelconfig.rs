use err::Error;
use nom::number::complete::{be_i16, be_i32, be_i64, be_i8, be_u8};
use nom::Needed;
#[allow(unused_imports)]
use nom::{
    bytes::complete::{tag, take, take_while_m_n},
    combinator::map_res,
    sequence::tuple,
};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

type NRes<'a, O> = nom::IResult<&'a [u8], O, err::Error>;

fn mkerr<'a, S, O>(msg: S) -> NRes<'a, O>
where
    S: Into<String>,
{
    let e = Error::with_msg(msg);
    Err(nom::Err::Error(e))
}

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
    pub split_count: i32,
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
    pub is_compressed: bool,
    pub is_shaped: bool,
    pub is_array: bool,
    pub is_big_endian: bool,
    pub compression_method: Option<CompressionMethod>,
    pub shape: Option<Vec<u32>>,
    pub source_name: Option<String>,
    unit: Option<String>,
    description: Option<String>,
    optional_fields: Option<String>,
    value_converter: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub format_version: i16,
    pub channel_name: String,
    pub entries: Vec<ConfigEntry>,
}

fn parse_short_string(inp: &[u8]) -> NRes<Option<String>> {
    let (inp, len1) = be_i32(inp)?;
    if len1 == -1 {
        return Ok((inp, None));
    }
    if len1 < 4 {
        return mkerr(format!("bad string len {}", len1));
    }
    if len1 > 500 {
        return mkerr(format!("large string len {}", len1));
    }
    let (inp, snb) = take((len1 - 4) as usize)(inp)?;
    match String::from_utf8(snb.to_vec()) {
        Ok(s1) => Ok((inp, Some(s1))),
        Err(e) => mkerr(format!("{:?}", e)),
    }
}

//pub fn parse_entry(inp: &[u8]) -> IResult<&[u8], Option<ConfigEntry>> {
pub fn parse_entry(inp: &[u8]) -> NRes<Option<ConfigEntry>> {
    let (inp, len1) = be_i32(inp)?;
    if len1 < 0 || len1 > 4000 {
        return mkerr(format!("ConfigEntry bad len1 {}", len1));
    }
    if inp.len() == 0 {
        return Ok((inp, None));
    }
    if inp.len() < len1 as usize - 4 {
        return Err(nom::Err::Incomplete(Needed::new(len1 as usize - 4)));
    }
    let inp_e = &inp[(len1 - 8) as usize..];
    let (inp, ts) = be_i64(inp)?;
    let (inp, pulse) = be_i64(inp)?;
    let (inp, ks) = be_i32(inp)?;
    let (inp, bs) = be_i64(inp)?;
    let (inp, split_count) = be_i32(inp)?;
    let (inp, status) = be_i32(inp)?;
    let (inp, bb) = be_i8(inp)?;
    let (inp, modulo) = be_i32(inp)?;
    let (inp, offset) = be_i32(inp)?;
    let (inp, precision) = be_i16(inp)?;
    let (inp, dtlen) = be_i32(inp)?;
    if dtlen > 100 {
        return mkerr(format!("unexpected data type len {}", dtlen));
    }
    let (inp, dtmask) = be_u8(inp)?;
    let is_compressed = dtmask & 0x80 != 0;
    let is_array = dtmask & 0x40 != 0;
    let is_big_endian = dtmask & 0x20 != 0;
    let is_shaped = dtmask & 0x10 != 0;
    let (inp, dtype) = be_i8(inp)?;
    if dtype > 13 {
        return mkerr(format!("unexpected data type {}", dtype));
    }
    let dtype = match num_traits::FromPrimitive::from_i8(dtype) {
        Some(k) => k,
        None => {
            return mkerr(format!("Can not convert {} to DType", dtype));
        }
    };
    let (inp, compression_method) = match is_compressed {
        false => (inp, None),
        true => {
            let (inp, cm) = be_u8(inp)?;
            match num_traits::FromPrimitive::from_u8(cm) {
                Some(k) => (inp, Some(k)),
                None => {
                    return mkerr(format!("unknown compression"));
                }
            }
        }
    };
    let (inp, shape) = match is_shaped {
        false => (inp, None),
        true => {
            let (mut inp, dim) = be_u8(inp)?;
            if dim > 4 {
                return mkerr(format!("unexpected number of dimensions: {}", dim));
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
    let (inp, source_name) = parse_short_string(inp)?;
    let (inp, unit) = parse_short_string(inp)?;
    let (inp, description) = parse_short_string(inp)?;
    let (inp, optional_fields) = parse_short_string(inp)?;
    let (inp, value_converter) = parse_short_string(inp)?;
    assert_eq!(inp.len(), inp_e.len());
    let (inp_e, len2) = be_i32(inp_e)?;
    if len1 != len2 {
        return mkerr(format!("mismatch  len1 {}  len2 {}", len1, len2));
    }
    Ok((
        inp_e,
        Some(ConfigEntry {
            ts,
            pulse,
            ks,
            bs,
            split_count: split_count,
            status,
            bb,
            modulo,
            offset,
            precision,
            dtype,
            is_compressed: is_compressed,
            is_array: is_array,
            is_shaped: is_shaped,
            is_big_endian: is_big_endian,
            compression_method: compression_method,
            shape,
            source_name: source_name,
            unit,
            description,
            optional_fields: optional_fields,
            value_converter: value_converter,
        }),
    ))
}

/*
Parse the full configuration file.
*/
pub fn parse_config(inp: &[u8]) -> NRes<Config> {
    let (inp, ver) = be_i16(inp)?;
    let (inp, len1) = be_i32(inp)?;
    if len1 <= 8 || len1 > 500 {
        return mkerr(format!("no channel name.  len1 {}", len1));
    }
    let (inp, chn) = take((len1 - 8) as usize)(inp)?;
    let (inp, len2) = be_i32(inp)?;
    if len1 != len2 {
        return mkerr(format!("Mismatch  len1 {}  len2 {}", len1, len2));
    }
    let mut entries = vec![];
    let mut inp_a = inp;
    while inp_a.len() > 0 {
        let inp = inp_a;
        let (inp, e) = parse_entry(inp)?;
        if let Some(e) = e {
            entries.push(e);
        }
        inp_a = inp;
    }
    let channel_name = match String::from_utf8(chn.to_vec()) {
        Ok(k) => k,
        Err(e) => {
            return mkerr(format!("channelName utf8 error {:?}", e));
        }
    };
    let ret = Config {
        format_version: ver,
        channel_name: channel_name,
        entries: entries,
    };
    Ok((inp, ret))
}

#[cfg(test)]
fn read_data() -> Vec<u8> {
    use std::io::Read;
    let path = "ks/config/S10CB01-RLOD100-PUP10:SIG-AMPLT/latest/00000_Config";
    let mut f1 = std::fs::File::open(path).unwrap();
    let mut buf = vec![];
    f1.read_to_end(&mut buf).unwrap();
    buf
}

#[test]
fn parse_dummy() {
    let config = parse_config(&[0, 0, 0, 0, 0, 11, 0x61, 0x62, 0x63, 0, 0, 0, 11, 0, 0, 0, 1]).unwrap();
    assert_eq!(0, config.1.format_version);
    assert_eq!("abc", config.1.channel_name);
}

#[test]
fn open_file() {
    let config = parse_config(&read_data()).unwrap().1;
    assert_eq!(0, config.format_version);
    assert_eq!(9, config.entries.len());
    for e in &config.entries {
        assert!(e.ts >= 631152000000000000);
        assert!(e.ts <= 1591106812800073974);
        assert!(e.shape.is_some());
    }
}
