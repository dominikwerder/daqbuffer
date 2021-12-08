use std::fmt;

#[derive(Debug)]
pub struct Error(::err::Error);

impl Error {
    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self(::err::Error::with_msg(s))
    }

    pub fn with_msg_no_trace<S: Into<String>>(s: S) -> Self {
        Self(::err::Error::with_msg_no_trace(s))
    }

    pub fn msg(&self) -> &str {
        self.0.msg()
    }

    pub fn reason(&self) -> Option<::err::Reason> {
        self.0.reason()
    }

    pub fn public_msg(&self) -> Option<&Vec<String>> {
        self.0.public_msg()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl std::error::Error for Error {}

impl From<::err::Error> for Error {
    fn from(x: ::err::Error) -> Self {
        Self(x)
    }
}

impl From<Error> for ::err::Error {
    fn from(x: Error) -> Self {
        x.0
    }
}

pub trait Convable {}

impl<T: Convable> From<T> for Error
where
    T: ToString,
{
    fn from(x: T) -> Self {
        Self(::err::Error::from_string(x))
    }
}

impl Convable for std::net::AddrParseError {}
impl Convable for std::string::FromUtf8Error {}
impl Convable for fmt::Error {}
impl Convable for std::io::Error {}
impl Convable for std::num::ParseIntError {}
impl Convable for dbconn::pg::Error {}
impl Convable for tokio::task::JoinError {}
impl Convable for tokio::time::error::Elapsed {}
impl Convable for serde_json::Error {}
impl Convable for chrono::ParseError {}
impl Convable for url::ParseError {}
impl Convable for http::uri::InvalidUri {}
impl Convable for http::Error {}
impl Convable for hyper::Error {}
