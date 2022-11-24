use httpret::api1::Api1ChannelHeader;
use netpod::log::*;
use nom::multi::many0;
use nom::number::complete::{be_u32, be_u64, be_u8};
use nom::IResult;
use std::num::NonZeroUsize;

// u32be length_1.
// there is exactly length_1 more bytes in this message.
// u8 mtype: 0: channel-header, 1: data

// for mtype == 0:
// The rest is a JSON with the channel header.

// for mtype == 1:
// u64be timestamp
// u64be pulse
// After that comes exactly (length_1 - 17) bytes of data.

#[derive(Debug)]
pub struct Header {
    header: Api1ChannelHeader,
}

impl Header {
    pub fn header(&self) -> &Api1ChannelHeader {
        &self.header
    }
}

#[derive(Debug)]
pub struct Data {
    ts: u64,
    pulse: u64,
    data: Vec<u8>,
}

impl Data {
    pub fn ts(&self) -> u64 {
        self.ts
    }

    pub fn pulse(&self) -> u64 {
        self.pulse
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug)]
pub enum Api1Frame {
    Header(Header),
    Data(Data),
}

fn header(inp: &[u8]) -> IResult<&[u8], Header> {
    match serde_json::from_slice(inp) {
        Ok(k) => {
            let k: Api1ChannelHeader = k;
            IResult::Ok((&inp[inp.len()..], Header { header: k }))
        }
        Err(e) => {
            error!("json header parse error: {e}");
            let e = nom::Err::Failure(nom::error::make_error(inp, nom::error::ErrorKind::Fail));
            IResult::Err(e)
        }
    }
}

fn data(inp: &[u8]) -> IResult<&[u8], Data> {
    if inp.len() < 16 {
        use nom::{Err, Needed};
        return IResult::Err(Err::Incomplete(Needed::Size(NonZeroUsize::new(16).unwrap())));
    }
    let (inp, ts) = be_u64(inp)?;
    let (inp, pulse) = be_u64(inp)?;
    let data = inp.into();
    let inp = &inp[inp.len()..];
    let res = Data { ts, pulse, data };
    IResult::Ok((inp, res))
}

fn api1_frame_complete(inp: &[u8]) -> IResult<&[u8], Api1Frame> {
    let (inp, mtype) = be_u8(inp)?;
    if mtype == 0 {
        let (inp, val) = header(inp)?;
        if inp.len() != 0 {
            // We did not consume the exact number of bytes
            let kind = nom::error::ErrorKind::Verify;
            let e = nom::error::Error::new(inp, kind);
            Err(nom::Err::Failure(e))
        } else {
            let res = Api1Frame::Header(val);
            IResult::Ok((inp, res))
        }
    } else if mtype == 1 {
        let (inp, val) = data(inp)?;
        if inp.len() != 0 {
            // We did not consume the exact number of bytes
            let kind = nom::error::ErrorKind::Verify;
            let e = nom::error::Error::new(inp, kind);
            Err(nom::Err::Failure(e))
        } else {
            let res = Api1Frame::Data(val);
            IResult::Ok((inp, res))
        }
    } else {
        let e = nom::Err::Incomplete(nom::Needed::Size(NonZeroUsize::new(1).unwrap()));
        IResult::Err(e)
    }
}

fn api1_frame(inp: &[u8]) -> IResult<&[u8], Api1Frame> {
    let (inp, len) = be_u32(inp)?;
    if len < 1 {
        use nom::error::{ErrorKind, ParseError};
        use nom::Err;
        return IResult::Err(Err::Failure(ParseError::from_error_kind(inp, ErrorKind::Fail)));
    }
    if inp.len() < len as usize {
        let e = nom::Err::Incomplete(nom::Needed::Size(NonZeroUsize::new(len as _).unwrap()));
        IResult::Err(e)
    } else {
        let (inp2, inp) = inp.split_at(len as _);
        let (inp2, res) = api1_frame_complete(inp2)?;
        if inp2.len() != 0 {
            let kind = nom::error::ErrorKind::Fail;
            let e = nom::error::Error::new(inp, kind);
            IResult::Err(nom::Err::Failure(e))
        } else {
            IResult::Ok((inp, res))
        }
    }
}

type Nres<'a, T> = IResult<&'a [u8], T, nom::error::VerboseError<&'a [u8]>>;

#[allow(unused)]
fn verbose_err(inp: &[u8]) -> Nres<u32> {
    use nom::error::{ErrorKind, ParseError, VerboseError};
    use nom::Err;
    let e = VerboseError::from_error_kind(inp, ErrorKind::Fail);
    return IResult::Err(Err::Failure(e));
}

pub fn api1_frames(inp: &[u8]) -> IResult<&[u8], Vec<Api1Frame>> {
    let (inp, res) = many0(api1_frame)(inp)?;
    IResult::Ok((inp, res))
}

#[test]
fn test_basic_frames() -> Result<(), err::Error> {
    use std::io::Write;
    let mut buf = Vec::new();
    let js = r#"{"name": "ch1", "type": "float64", "byteOrder": "LITTLE_ENDIAN"}"#;
    buf.write(&(1 + js.as_bytes().len() as u32).to_be_bytes())?;
    buf.write(&[0])?;
    buf.write(js.as_bytes())?;

    buf.write(&25u32.to_be_bytes())?;
    buf.write(&[1])?;
    buf.write(&20u64.to_be_bytes())?;
    buf.write(&21u64.to_be_bytes())?;
    buf.write(&5.123f64.to_be_bytes())?;

    buf.write(&25u32.to_be_bytes())?;
    buf.write(&[1])?;
    buf.write(&22u64.to_be_bytes())?;
    buf.write(&23u64.to_be_bytes())?;
    buf.write(&7.88f64.to_be_bytes())?;

    match api1_frames(&buf) {
        Ok((_, frames)) => {
            assert_eq!(frames.len(), 3);
        }
        Err(e) => {
            error!("can not parse result: {e}");
            panic!()
        }
    };
    Ok(())
}
