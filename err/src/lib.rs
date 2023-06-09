//! Error handling and reporting.

use serde::{Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::convert::Infallible;
use std::fmt;
use std::net::AddrParseError;
use std::num::{ParseFloatError, ParseIntError};
use std::string::FromUtf8Error;
use std::sync::PoisonError;

pub mod bt {
    pub use backtrace::Backtrace;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Reason {
    InternalError,
    BadRequest,
    IoError,
}

/// The common error type for this application.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Error {
    msg: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace_str: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    public_msg: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<Reason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<Box<Error>>,
}

impl Error {
    pub fn with_msg_no_trace<S: Into<String>>(s: S) -> Self {
        Self {
            msg: s.into(),
            trace_str: None,
            public_msg: None,
            reason: None,
            parent: None,
        }
    }

    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self::with_msg_no_trace(s).add_backtrace()
    }

    pub fn with_public_msg_no_trace<S: Into<String>>(s: S) -> Self {
        let s = s.into();
        let ret = Self::with_msg_no_trace(&s);
        let ret = ret.add_public_msg(s);
        ret
    }

    pub fn with_public_msg<S: Into<String>>(s: S) -> Self {
        let s = s.into();
        let ret = Self::with_msg_no_trace(String::new());
        let ret = ret.add_backtrace();
        let ret = ret.add_public_msg(s);
        ret
    }

    pub fn from_string<E>(e: E) -> Self
    where
        E: ToString,
    {
        Self::with_msg(e.to_string())
    }

    pub fn add_backtrace(self) -> Self {
        let mut ret = self;
        ret.trace_str = Some(fmt_backtrace(&backtrace::Backtrace::new()));
        ret
    }

    pub fn mark_bad_request(mut self) -> Self {
        self.reason = Some(Reason::BadRequest);
        self
    }

    pub fn mark_io_error(mut self) -> Self {
        self.reason = Some(Reason::IoError);
        self
    }

    pub fn add_public_msg(mut self, msg: impl Into<String>) -> Self {
        if self.public_msg.is_none() {
            self.public_msg = Some(Vec::new());
        }
        self.public_msg.as_mut().unwrap().push(msg.into());
        self
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn public_msg(&self) -> Option<&Vec<String>> {
        self.public_msg.as_ref()
    }

    pub fn reason(&self) -> Option<Reason> {
        self.reason.clone()
    }

    pub fn to_public_error(&self) -> PublicError {
        PublicError {
            reason: self.reason(),
            msg: self
                .public_msg()
                .map(|k| k.join("\n"))
                .unwrap_or("No error message".into()),
        }
    }
}

fn fmt_backtrace(trace: &backtrace::Backtrace) -> String {
    use std::io::Write;
    let mut buf = Vec::new();
    let mut c1 = 0;
    'outer: for fr in trace.frames() {
        for sy in fr.symbols() {
            let is_ours = match sy.filename() {
                None => false,
                Some(s) => {
                    let s = s.to_str().unwrap();
                    s.contains("/dev/daqbuffer/") || s.contains("/build/daqbuffer/")
                }
            };
            let name = match sy.name() {
                Some(k) => k.to_string(),
                _ => "[err]".into(),
            };
            let filename = match sy.filename() {
                Some(k) => match k.to_str() {
                    Some(k) => k,
                    _ => "[err]",
                },
                _ => "[err]",
            };
            let lineno = match sy.lineno() {
                Some(k) => k,
                _ => 0,
            };
            if is_ours {
                write!(&mut buf, "\n    {name}\n      {filename}  {lineno}").unwrap();
                c1 += 1;
                if c1 >= 10 {
                    break 'outer;
                }
            }
        }
    }
    String::from_utf8(buf).unwrap()
}

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let trace_str = if let Some(s) = &self.trace_str {
            s.into()
        } else {
            String::new()
        };
        write!(fmt, "msg: {}", self.msg)?;
        if let Some(msgs) = self.public_msg() {
            for msg in msgs {
                write!(fmt, "\npublic: {}", msg)?;
            }
        }
        if !trace_str.is_empty() {
            write!(fmt, "\nTrace:\n{}", trace_str)?;
        }
        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl std::error::Error for Error {}

pub trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T, E> ErrConv<T> for Result<T, E>
where
    E: Into<Error>,
{
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(e.into()),
        }
    }
}

pub trait ErrStr<T> {
    fn errstr(self) -> Result<T, Error>;
}

impl<T, E> ErrStr<T> for Result<T, E>
where
    E: ToString,
{
    fn errstr(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(e.to_string())),
        }
    }
}

pub trait ToErr {
    fn to_err(self) -> Error;
}

impl<T: ToErr> From<T> for Error {
    fn from(k: T) -> Self {
        k.to_err()
    }
}

impl From<PublicError> for Error {
    fn from(k: PublicError) -> Self {
        Self {
            msg: String::new(),
            trace_str: None,
            public_msg: Some(vec![k.msg().into()]),
            reason: k.reason(),
            parent: None,
        }
    }
}

impl ToErr for Infallible {
    fn to_err(self) -> Error {
        Error::with_msg_no_trace(String::new())
    }
}

impl From<String> for Error {
    fn from(k: String) -> Self {
        Self::with_msg(k)
    }
}

impl From<&str> for Error {
    fn from(k: &str) -> Self {
        Self::with_msg(k)
    }
}

impl From<std::io::Error> for Error {
    fn from(k: std::io::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<AddrParseError> for Error {
    fn from(k: AddrParseError) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(k: serde_json::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(k: async_channel::SendError<T>) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(k: async_channel::RecvError) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(k: chrono::format::ParseError) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<ParseIntError> for Error {
    fn from(k: ParseIntError) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<ParseFloatError> for Error {
    fn from(k: ParseFloatError) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(k: FromUtf8Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(k: std::str::Utf8Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<serde_cbor::Error> for Error {
    fn from(k: serde_cbor::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<erased_serde::Error> for Error {
    fn from(k: erased_serde::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<std::fmt::Error> for Error {
    fn from(k: std::fmt::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<regex::Error> for Error {
    fn from(k: regex::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::with_msg("PoisonError")
    }
}

impl From<url::ParseError> for Error {
    fn from(k: url::ParseError) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

impl From<TryFromSliceError> for Error {
    fn from(k: TryFromSliceError) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(k: rmp_serde::encode::Error) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(k: rmp_serde::decode::Error) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

impl From<http::header::ToStrError> for Error {
    fn from(k: http::header::ToStrError) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublicError {
    reason: Option<Reason>,
    msg: String,
}

impl PublicError {
    pub fn reason(&self) -> Option<Reason> {
        self.reason.clone()
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }
}

// TODO make this more useful
impl From<Error> for PublicError {
    fn from(k: Error) -> Self {
        Self {
            reason: k.reason(),
            msg: k.msg().into(),
        }
    }
}

impl From<&Error> for PublicError {
    fn from(k: &Error) -> Self {
        Self {
            reason: k.reason(),
            msg: k.msg().into(),
        }
    }
}

pub fn todo() {
    let bt = backtrace::Backtrace::new();
    eprintln!("TODO\n{bt:?}");
    todo!("TODO\n{bt:?}");
}

pub fn todoval<T>() -> T {
    let bt = backtrace::Backtrace::new();
    eprintln!("TODO\n{bt:?}");
    todo!("TODO todoval\n{bt:?}")
}
