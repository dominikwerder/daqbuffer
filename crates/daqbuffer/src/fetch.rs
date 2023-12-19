use futures_util::future;
use futures_util::StreamExt;
use http::header;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::connect_client;
use httpclient::http;
use httpclient::hyper::Request;
use httpclient::IncomingStream;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use std::fmt;
use streams::cbor::FramedBytesToSitemtyDynEventsStream;
use url::Url;

pub struct Error {}

impl<T> From<T> for Error
where
    T: fmt::Debug,
{
    fn from(_value: T) -> Self {
        Self {}
    }
}

pub async fn fetch_cbor(url: &str, scalar_type: ScalarType, shape: Shape) -> Result<(), Error> {
    let url: Url = url.parse().unwrap();
    let accept = "application/cbor";
    let req = Request::builder()
        .method(Method::GET)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| "NoHostname")?)
        .header(header::ACCEPT, accept)
        .body(body_empty())?;
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    let (head, body) = res.into_parts();
    debug!("http_get  head {head:?}");
    let stream = IncomingStream::new(body);
    let stream = FramedBytesToSitemtyDynEventsStream::new(stream, scalar_type, shape);
    let stream = stream.map(|item| info!("{item:?}"));
    stream.for_each(|_| future::ready(())).await;
    Ok(())
}
