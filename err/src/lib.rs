use nom::error::ErrorKind;
use std::fmt::Debug;
use std::num::ParseIntError;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub struct Error {
    msg: String,
}

impl Error {
    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self { msg: s.into() }
    }
}

impl From<String> for Error {
    fn from(k: String) -> Self {
        Self { msg: k }
    }
}

impl From<&str> for Error {
    fn from(k: &str) -> Self {
        Self { msg: k.into() }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Error")
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(k: std::io::Error) -> Self {
        Self { msg: k.to_string() }
    }
}

impl From<http::Error> for Error {
    fn from(k: http::Error) -> Self {
        Self { msg: k.to_string() }
    }
}

impl From<hyper::Error> for Error {
    fn from(k: hyper::Error) -> Self {
        Self { msg: k.to_string() }
    }
}

impl From<serde_json::Error> for Error {
    fn from(k: serde_json::Error) -> Self {
        Self { msg: k.to_string() }
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(k: async_channel::RecvError) -> Self {
        Self { msg: k.to_string() }
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(k: chrono::format::ParseError) -> Self {
        Self { msg: k.to_string() }
    }
}

impl From<ParseIntError> for Error {
    fn from(k: ParseIntError) -> Self {
        Self { msg: k.to_string() }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(k: FromUtf8Error) -> Self {
        Self { msg: k.to_string() }
    }
}

impl<T> From<nom::Err<T>> for Error {
    fn from(k: nom::Err<T>) -> Self {
        Self {
            msg: format!("nom::Err<T>"),
        }
    }
}

impl<I> From<nom::error::VerboseError<I>> for Error {
    fn from(k: nom::error::VerboseError<I>) -> Self {
        Self {
            msg: format!("nom::error::VerboseError<I>"),
        }
    }
}

impl<I> nom::error::ParseError<I> for Error {
    fn from_error_kind(input: I, kind: ErrorKind) -> Self {
        todo!()
    }

    fn append(input: I, kind: ErrorKind, other: Self) -> Self {
        todo!()
    }
}
