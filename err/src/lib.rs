#[derive(Debug)]
pub struct Error {
    msg: String,
}

impl Error {
    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self {
            msg: s.into(),
        }
    }
}

impl From<String> for Error {
    fn from(k: String) -> Self {
        Self {
            msg: k,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Error")
    }
}

impl std::error::Error for Error {
}

impl From<std::io::Error> for Error {
    fn from (k: std::io::Error) -> Self {
        Self {
            msg: k.to_string(),
        }
    }
}

impl From<http::Error> for Error {
    fn from (k: http::Error) -> Self {
        Self {
            msg: k.to_string(),
        }
    }
}

impl From<hyper::Error> for Error {
    fn from (k: hyper::Error) -> Self {
        Self {
            msg: k.to_string(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from (k: serde_json::Error) -> Self {
        Self {
            msg: k.to_string(),
        }
    }
}

impl From<async_channel::RecvError> for Error {
    fn from (k: async_channel::RecvError) -> Self {
        Self {
            msg: k.to_string(),
        }
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from (k: chrono::format::ParseError) -> Self {
        Self {
            msg: k.to_string(),
        }
    }
}
