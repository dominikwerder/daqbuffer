use futures_util::future;
use futures_util::StreamExt;
use http::header;
use http::Method;
use httpclient::body_empty;
use httpclient::connect_client;
use httpclient::http;
use httpclient::hyper::Request;
use httpclient::IncomingStream;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use netpod::APP_CBOR_FRAMES;
use std::fmt;
use streams::cbor::FramedBytesToSitemtyDynEventsStream;
use url::Url;

pub struct Error {
    msg: String,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.msg)
    }
}

impl<T> From<T> for Error
where
    T: fmt::Debug,
{
    fn from(value: T) -> Self {
        Self {
            msg: format!("{value:?}"),
        }
    }
}

pub async fn fetch_cbor(url: &str, scalar_type: ScalarType, shape: Shape) -> Result<(), Error> {
    let url: Url = url.parse()?;
    debug!("parsed url: {url:?}");
    let req = Request::builder()
        .method(Method::GET)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| "NoHostname")?)
        .header(header::ACCEPT, APP_CBOR_FRAMES)
        .body(body_empty())?;
    debug!("open connection to {:?}", req.uri());
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    let (head, body) = res.into_parts();
    debug!("fetch_cbor  head {head:?}");
    let stream = IncomingStream::new(body);
    let stream = FramedBytesToSitemtyDynEventsStream::new(stream, scalar_type, shape);
    let stream = stream.map(|item| info!("{item:?}"));
    stream.for_each(|_| future::ready(())).await;
    Ok(())
}
