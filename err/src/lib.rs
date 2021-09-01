/*!
Error handling and reporting.
*/

use http::header::InvalidHeaderValue;
use http::uri::InvalidUri;
use nom::error::ErrorKind;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::net::AddrParseError;
use std::num::{ParseFloatError, ParseIntError};
use std::string::FromUtf8Error;
use std::sync::PoisonError;
use tokio::task::JoinError;
use tokio::time::error::Elapsed;

/**
The common error type for this application.
*/
#[derive(Serialize, Deserialize)]
pub struct Error {
    msg: String,
    #[serde(skip)]
    trace: Option<backtrace::Backtrace>,
    trace_str: Option<String>,
}

impl Error {
    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self {
            msg: s.into(),
            trace: None,
            trace_str: Some(fmt_backtrace(&backtrace::Backtrace::new())),
        }
    }

    pub fn with_msg_no_trace<S: Into<String>>(s: S) -> Self {
        Self {
            msg: s.into(),
            trace: None,
            trace_str: None,
        }
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }
}

fn fmt_backtrace(trace: &backtrace::Backtrace) -> String {
    use std::io::Write;
    let mut buf = vec![];
    let mut c1 = 0;
    'outer: for fr in trace.frames() {
        for sy in fr.symbols() {
            let is_ours = match sy.filename() {
                None => false,
                Some(s) => {
                    let s = s.to_str().unwrap();
                    s.contains("/dev/daqbuffer/") || s.contains("/data_meta/build/")
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
                write!(&mut buf, "\n    {}\n      {}  {}", name, filename, lineno).unwrap();
                c1 += 1;
                if c1 >= 10 {
                    break 'outer;
                }
            }
        }
    }
    String::from_utf8(buf).unwrap()
}

impl std::fmt::Debug for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        let trace_str = if let Some(trace) = &self.trace {
            fmt_backtrace(trace)
        } else if let Some(s) = &self.trace_str {
            s.into()
        } else {
            "NOTRACE".into()
        };
        write!(fmt, "Error {}\nTrace:\n{}", self.msg, trace_str)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, fmt)
    }
}

impl std::error::Error for Error {}

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

impl From<http::Error> for Error {
    fn from(k: http::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<hyper::Error> for Error {
    fn from(k: hyper::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(k: serde_json::Error) -> Self {
        Self::with_msg(k.to_string())
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

impl<T: Debug> From<nom::Err<T>> for Error {
    fn from(k: nom::Err<T>) -> Self {
        Self::with_msg(format!("nom::Err<T> {:?}", k))
    }
}

impl<I> nom::error::ParseError<I> for Error {
    fn from_error_kind(_input: I, kind: ErrorKind) -> Self {
        Self::with_msg(format!("ParseError  {:?}", kind))
    }

    fn append(_input: I, kind: ErrorKind, other: Self) -> Self {
        Self::with_msg(format!("ParseError  kind {:?}  other {:?}", kind, other))
    }
}

impl From<JoinError> for Error {
    fn from(k: JoinError) -> Self {
        Self::with_msg(format!("JoinError {:?}", k))
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(k: Box<bincode::ErrorKind>) -> Self {
        Self::with_msg(format!("bincode::ErrorKind {:?}", k))
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(k: tokio_postgres::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(k: async_channel::SendError<T>) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<InvalidUri> for Error {
    fn from(k: InvalidUri) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<serde_cbor::Error> for Error {
    fn from(k: serde_cbor::Error) -> Self {
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

impl From<InvalidHeaderValue> for Error {
    fn from(k: InvalidHeaderValue) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

impl From<Elapsed> for Error {
    fn from(k: Elapsed) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

impl From<url::ParseError> for Error {
    fn from(k: url::ParseError) -> Self {
        Self::with_msg(format!("{:?}", k))
    }
}

pub fn todo() {
    todo!("TODO");
}

pub fn todoval<T>() -> T {
    todo!("TODO todoval")
}
