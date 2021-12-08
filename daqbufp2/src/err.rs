pub trait ErrConv<T> {
    fn ec(self) -> Result<T, ::err::Error>;
}

pub trait Convable: ToString {}

impl<T, E: Convable> ErrConv<T> for Result<T, E> {
    fn ec(self) -> Result<T, err::Error> {
        match self {
            Ok(x) => Ok(x),
            Err(e) => Err(::err::Error::from_string(e.to_string())),
        }
    }
}

impl Convable for http::Error {}
impl Convable for hyper::Error {}
impl Convable for tokio::task::JoinError {}
