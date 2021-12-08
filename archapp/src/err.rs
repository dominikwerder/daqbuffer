use std::fmt;

pub struct ArchError(::err::Error);

impl ArchError {
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

impl fmt::Debug for ArchError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(fmt)
    }
}

impl fmt::Display for ArchError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl std::error::Error for ArchError {}

impl From<::err::Error> for ArchError {
    fn from(x: ::err::Error) -> Self {
        Self(x)
    }
}

impl From<ArchError> for ::err::Error {
    fn from(x: ArchError) -> Self {
        x.0
    }
}

impl From<std::string::FromUtf8Error> for ArchError {
    fn from(k: std::string::FromUtf8Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<std::io::Error> for ArchError {
    fn from(k: std::io::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl<T> From<async_channel::SendError<T>> for ArchError {
    fn from(k: async_channel::SendError<T>) -> Self {
        Self::with_msg(k.to_string())
    }
}

impl From<serde_json::Error> for ArchError {
    fn from(k: serde_json::Error) -> Self {
        Self::with_msg(k.to_string())
    }
}
