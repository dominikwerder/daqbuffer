use std::fmt;

pub struct Error(err::Error);

impl Error {
    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self(err::Error::with_msg(s))
    }

    pub fn with_msg_no_trace<S: Into<String>>(s: S) -> Self {
        Self(err::Error::with_msg_no_trace(s))
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(fmt)
    }
}

impl From<Error> for err::Error {
    fn from(x: Error) -> Self {
        x.0
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(k: std::string::FromUtf8Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(k: std::io::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(k: async_channel::SendError<T>) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(k: serde_json::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}
