use err::Error;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::DAY;
use netpod::timeunits::MS;
use netpod::ByteOrder;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::TsNano;
use nom::bytes::complete::take;
use nom::number::complete::be_i16;
use nom::number::complete::be_i32;
use nom::number::complete::be_i64;
use nom::number::complete::be_i8;
use nom::number::complete::be_u8;
use nom::Needed;
use num_derive::FromPrimitive;
use num_derive::ToPrimitive;
use num_traits::ToPrimitive;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use tokio::io::ErrorKind;

const TEST_BACKEND: &str = "testbackend-00";

#[derive(Debug)]
pub struct NErr {
    msg: String,
}

impl<T: fmt::Debug> From<nom::Err<T>> for NErr {
    fn from(k: nom::Err<T>) -> Self {
        Self {
            msg: format!("nom::Err<T> {:?}", k),
        }
    }
}

impl<I> nom::error::ParseError<I> for NErr {
    fn from_error_kind(_input: I, kind: nom::error::ErrorKind) -> Self {
        Self {
            msg: format!("ParseError  {:?}", kind),
        }
    }

    fn append(_input: I, kind: nom::error::ErrorKind, other: Self) -> Self {
        Self {
            msg: format!("ParseError  kind {:?}  other {:?}", kind, other),
        }
    }
}

impl From<NErr> for Error {
    fn from(x: NErr) -> Self {
        Self::with_msg_no_trace(x.msg)
    }
}

type NRes<'a, O> = nom::IResult<&'a [u8], O, NErr>;

fn mkerr<'a, S, O>(msg: S) -> NRes<'a, O>
where
    S: Into<String>,
{
    let e = NErr { msg: msg.into() };
    Err(nom::Err::Error(e))
}

#[derive(Clone, Debug, FromPrimitive, ToPrimitive, Serialize, Deserialize)]
pub enum CompressionMethod {
    BitshuffleLZ4 = 0,
}

impl CompressionMethod {
    pub fn to_i16(&self) -> i16 {
        ToPrimitive::to_i16(self).unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub ts: TsNano,
    pub pulse: i64,
    pub ks: i32,
    pub bs: TsNano,
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
    pub scalar_type: ScalarType,
    pub is_compressed: bool,
    pub is_shaped: bool,
    pub is_array: bool,
    pub byte_order: ByteOrder,
    pub compression_method: Option<CompressionMethod>,
    pub shape: Option<Vec<u32>>,
    pub source_name: Option<String>,
    pub unit: Option<String>,
    pub description: Option<String>,
    pub optional_fields: Option<String>,
    pub value_converter: Option<String>,
}

impl ConfigEntry {
    pub fn to_shape(&self) -> Result<Shape, Error> {
        let ret = match &self.shape {
            Some(lens) => {
                if lens.len() == 1 {
                    Shape::Wave(lens[0])
                } else if lens.len() == 2 {
                    Shape::Image(lens[0], lens[1])
                } else {
                    // TODO
                    // Need a new Shape variant for images.
                    return Err(Error::with_msg(format!("Channel config unsupported shape {:?}", self)))?;
                }
            }
            None => Shape::Scalar,
        };
        Ok(ret)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfigs {
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
    let bs = TsNano(bs as u64 * MS);
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
    let byte_order = ByteOrder::from_dtype_flags(dtmask);
    let is_shaped = dtmask & 0x10 != 0;
    let (inp, dtype) = be_u8(inp)?;
    if dtype > 13 {
        return mkerr(format!("unexpected data type {}", dtype));
    }
    let scalar_type = match ScalarType::from_dtype_index(dtype) {
        Ok(k) => k,
        Err(e) => {
            return mkerr(format!("Can not convert {} to DType  {:?}", dtype, e));
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
            ts: TsNano::from_ns(ts as u64),
            pulse,
            ks,
            bs,
            split_count: split_count,
            status,
            bb,
            modulo,
            offset,
            precision,
            scalar_type,
            is_compressed: is_compressed,
            is_array: is_array,
            is_shaped: is_shaped,
            byte_order,
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

/**
Parse a complete configuration file from given in-memory input buffer.
*/
pub fn parse_config(inp: &[u8]) -> NRes<ChannelConfigs> {
    let (inp, ver) = be_i16(inp)?;
    let (inp, len1) = be_i32(inp)?;
    if len1 <= 8 || len1 > 500 {
        return mkerr(format!("no channel name.  len1 {}", len1));
    }
    let (inp, chn) = take((len1 - 8) as usize)(inp)?;
    let channel_name = match String::from_utf8(chn.to_vec()) {
        Ok(k) => k,
        Err(e) => {
            return mkerr(format!("channelName utf8 error {:?}", e));
        }
    };
    let (inp, len2) = be_i32(inp)?;
    if len1 != len2 {
        return mkerr(format!("Mismatch  len1 {}  len2 {}", len1, len2));
    }
    let mut entries = Vec::new();
    let mut inp_a = inp;
    while inp_a.len() > 0 {
        let inp = inp_a;
        let (inp, e) = parse_entry(inp)?;
        if let Some(e) = e {
            entries.push(e);
        }
        inp_a = inp;
    }
    // TODO hack to accommodate for parts of databuffer nodes failing:
    if false && channel_name == "SARFE10-PSSS059:SPECTRUM_X" {
        warn!("apply hack for {channel_name}");
        entries = entries
            .into_iter()
            .map(|mut e| {
                e.is_array = true;
                e.shape = Some(vec![2560]);
                e
            })
            .collect();
    }
    if false && channel_name == "SARFE10-PSSS059:SPECTRUM_Y" {
        warn!("apply hack for {channel_name}");
        entries = entries
            .into_iter()
            .map(|mut e| {
                e.is_array = true;
                e.shape = Some(vec![2560]);
                e
            })
            .collect();
    }
    let ret = ChannelConfigs {
        format_version: ver,
        channel_name,
        entries,
    };
    Ok((inp, ret))
}

pub async fn channel_config(q: &ChannelConfigQuery, ncc: &NodeConfigCached) -> Result<ChannelConfigResponse, Error> {
    let conf = read_local_config(q.channel.clone(), ncc.clone()).await?;
    let entry_res = extract_matching_config_entry(&q.range, &conf)?;
    let entry = match entry_res {
        MatchingConfigEntry::None => return Err(Error::with_public_msg("no config entry found")),
        MatchingConfigEntry::Multiple => return Err(Error::with_public_msg("multiple config entries found")),
        MatchingConfigEntry::Entry(entry) => entry,
    };
    let ret = ChannelConfigResponse {
        channel: q.channel.clone(),
        scalar_type: entry.scalar_type.clone(),
        byte_order: Some(entry.byte_order.clone()),
        shape: entry.to_shape()?,
    };
    Ok(ret)
}

async fn read_local_config_real(channel: SfDbChannel, ncc: &NodeConfigCached) -> Result<ChannelConfigs, Error> {
    let path = ncc
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
        .data_base_path
        .join("config")
        .join(&channel.name)
        .join("latest")
        .join("00000_Config");
    // TODO use commonio here to wrap the error conversion
    let buf = match tokio::fs::read(&path).await {
        Ok(k) => k,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                let bt = err::bt::Backtrace::new();
                netpod::log::error!("{bt:?}");
                return Err(Error::with_public_msg(format!(
                    "databuffer channel config file not found for channel {channel:?} at {path:?}"
                )));
            }
            ErrorKind::PermissionDenied => {
                return Err(Error::with_public_msg(format!(
                    "databuffer channel config file permission denied for channel {channel:?} at {path:?}"
                )))
            }
            _ => return Err(e.into()),
        },
    };
    let config = parse_config(&buf).map_err(NErr::from)?;
    Ok(config.1)
}

async fn read_local_config_test(channel: SfDbChannel, ncc: &NodeConfigCached) -> Result<ChannelConfigs, Error> {
    if channel.name() == "test-gen-i32-dim0-v00" {
        let ret = ChannelConfigs {
            format_version: 0,
            channel_name: channel.name().into(),
            entries: vec![ConfigEntry {
                ts: TsNano(0),
                pulse: 0,
                ks: 2,
                bs: TsNano(DAY),
                split_count: ncc.node_config.cluster.nodes.len() as _,
                status: -1,
                bb: -1,
                modulo: -1,
                offset: -1,
                precision: -1,
                scalar_type: ScalarType::I32,
                is_compressed: false,
                is_shaped: false,
                is_array: false,
                byte_order: ByteOrder::Big,
                compression_method: None,
                shape: None,
                source_name: None,
                unit: None,
                description: None,
                optional_fields: None,
                value_converter: None,
            }],
        };
        Ok(ret)
    } else if channel.name() == "test-gen-i32-dim0-v01" {
        let ret = ChannelConfigs {
            format_version: 0,
            channel_name: channel.name().into(),
            entries: vec![ConfigEntry {
                ts: TsNano(0),
                pulse: 0,
                ks: 2,
                bs: TsNano(DAY),
                split_count: ncc.node_config.cluster.nodes.len() as _,
                status: -1,
                bb: -1,
                modulo: -1,
                offset: -1,
                precision: -1,
                scalar_type: ScalarType::I32,
                is_compressed: false,
                is_shaped: false,
                is_array: false,
                byte_order: ByteOrder::Big,
                compression_method: None,
                shape: None,
                source_name: None,
                unit: None,
                description: None,
                optional_fields: None,
                value_converter: None,
            }],
        };
        Ok(ret)
    } else {
        Err(Error::with_msg_no_trace(format!("unknown test channel {channel:?}")))
    }
}

// TODO can I take parameters as ref, even when used in custom streams?
pub async fn read_local_config(channel: SfDbChannel, ncc: NodeConfigCached) -> Result<ChannelConfigs, Error> {
    if channel.backend() == TEST_BACKEND {
        read_local_config_test(channel, &ncc).await
    } else {
        read_local_config_real(channel, &ncc).await
    }
}

#[derive(Clone)]
pub enum MatchingConfigEntry<'a> {
    None,
    Multiple,
    Entry(&'a ConfigEntry),
}

pub fn extract_matching_config_entry<'a>(
    range: &NanoRange,
    channel_config: &'a ChannelConfigs,
) -> Result<MatchingConfigEntry<'a>, Error> {
    // TODO remove temporary
    if channel_config.channel_name == "SARFE10-PSSS059:SPECTRUM_X" {
        debug!("found config {:?}", channel_config);
    }
    let mut ixs = Vec::new();
    for i1 in 0..channel_config.entries.len() {
        let e1 = &channel_config.entries[i1];
        if i1 + 1 < channel_config.entries.len() {
            let e2 = &channel_config.entries[i1 + 1];
            if e1.ts.ns() < range.end && e2.ts.ns() >= range.beg {
                ixs.push(i1);
            }
        } else {
            if e1.ts.ns() < range.end {
                ixs.push(i1);
            }
        }
    }
    if ixs.len() == 0 {
        Ok(MatchingConfigEntry::None)
    } else if ixs.len() > 1 {
        Ok(MatchingConfigEntry::Multiple)
    } else {
        let e = &channel_config.entries[ixs[0]];
        // TODO remove temporary
        if channel_config.channel_name == "SARFE10-PSSS059:SPECTRUM_X" {
            debug!("found matching entry {:?}", e);
        }
        Ok(MatchingConfigEntry::Entry(e))
    }
}

#[cfg(test)]
mod test {
    use super::parse_config;

    fn read_data() -> Vec<u8> {
        use std::io::Read;
        //let path = "ks/config/S10CB01-RLOD100-PUP10:SIG-AMPLT/latest/00000_Config";
        let cwd = std::env::current_dir();
        netpod::log::info!("CWD: {:?}", cwd);
        let path = "../resources/sf-daqbuf-33-S10CB01-RLOD100-PUP10:SIG-AMPLT-latest-00000_Config";
        //let path = "../resources/sf-daqbuf-21-S10CB01-RLOD100-PUP10:SIG-AMPLT-latest-00000_Config";
        let mut f1 = std::fs::File::open(path).unwrap();
        let mut buf = Vec::new();
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
        assert_eq!(config.format_version, 0);
        assert_eq!(config.entries.len(), 18);
        for e in &config.entries {
            assert!(e.ts.ns() >= 631152000000000000);
            assert!(e.ts.ns() <= 1613640673424172164);
            assert!(e.shape.is_some());
        }
    }
}
