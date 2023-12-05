pub use hyper_util;

pub use http_body_util;
pub use http_body_util::Full;

use bytes::Bytes;
use bytes::BytesMut;
use err::PublicError;
use futures_util::pin_mut;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use http_body_util::combinators::BoxBody;
use hyper::body::Body;
use hyper::client::conn::http2::SendRequest;
use hyper::Method;
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::NodeConfigCached;
use netpod::APP_JSON;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
use url::Url;

pub type BodyBox = BoxBody<Bytes, BodyError>;
pub type RespBox = Response<BodyBox>;

#[derive(Debug)]
pub enum BodyError {
    Bad,
}

impl fmt::Display for BodyError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("Bad")
    }
}

impl std::error::Error for BodyError {}

impl From<std::convert::Infallible> for BodyError {
    fn from(_value: std::convert::Infallible) -> Self {
        BodyError::Bad
    }
}

#[derive(Debug)]
pub enum Error {
    BadUrl,
    Connection,
    IO,
    Http,
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO
    }
}

impl From<http::Error> for Error {
    fn from(value: http::Error) -> Self {
        Self::Http
    }
}

impl From<hyper::Error> for Error {
    fn from(value: hyper::Error) -> Self {
        Self::Http
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl err::ToErr for Error {
    fn to_err(self) -> err::Error {
        err::Error::with_msg_no_trace(format!("self"))
    }
}

pub struct HttpResponse {
    pub head: http::response::Parts,
    pub body: Bytes,
}

pub async fn http_get(url: Url, accept: &str) -> Result<HttpResponse, Error> {
    let req = Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| Error::BadUrl)?)
        .header(header::ACCEPT, accept)
        .body(Full::new(Bytes::new()))?;
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    let (head, mut body) = res.into_parts();
    debug!("http_get  head {head:?}");
    use bytes::BufMut;
    use http_body_util::BodyExt;
    let mut buf = BytesMut::new();
    while let Some(x) = body.frame().await {
        match x {
            Ok(mut x) => {
                if let Some(x) = x.data_mut() {
                    buf.put(x);
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    let ret = HttpResponse {
        head,
        body: buf.freeze(),
    };
    Ok(ret)
}

pub async fn http_post(url: Url, accept: &str, body: String) -> Result<Bytes, Error> {
    let body = Bytes::from(body.as_bytes().to_vec());
    let req = Request::builder()
        .method(http::Method::POST)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| Error::BadUrl)?)
        .header(header::CONTENT_TYPE, APP_JSON)
        .header(header::ACCEPT, accept)
        .body(Full::new(body))?;
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        let (_head, body) = res.into_parts();
        let buf = read_body_bytes(body).await?;
        let s = String::from_utf8_lossy(&buf);
        return Err(Error::Http);
    }
    let (head, mut body) = res.into_parts();
    debug!("http_get  head {head:?}");
    use bytes::BufMut;
    use http_body_util::BodyExt;
    let mut buf = BytesMut::new();
    while let Some(x) = body.frame().await {
        match x {
            Ok(mut x) => {
                if let Some(x) = x.data_mut() {
                    buf.put(x);
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    let buf = read_body_bytes(body).await?;
    Ok(buf)
}

pub async fn connect_client(uri: &http::Uri) -> Result<SendRequest<Full<Bytes>>, Error> {
    let host = uri.host().ok_or_else(|| Error::BadUrl)?;
    let port = uri.port_u16().ok_or_else(|| Error::BadUrl)?;
    let stream = TcpStream::connect(format!("{host}:{port}")).await?;
    let executor = hyper_util::rt::TokioExecutor::new();
    let (send_req, conn) = hyper::client::conn::http2::Builder::new(executor)
        .handshake(hyper_util::rt::TokioIo::new(stream))
        .await?;
    // TODO would need to take greater care of this task to catch connection-level errors.
    tokio::spawn(conn);
    Ok(send_req)
}

pub async fn read_body_bytes(mut body: hyper::body::Incoming) -> Result<Bytes, Error> {
    use bytes::BufMut;
    use http_body_util::BodyExt;
    let mut buf = BytesMut::new();
    while let Some(x) = body.frame().await {
        match x {
            Ok(mut x) => {
                if let Some(x) = x.data_mut() {
                    buf.put(x);
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(buf.freeze())
}

pub struct IncomingStream {
    inp: hyper::body::Incoming,
}

impl IncomingStream {
    pub fn new(inp: hyper::body::Incoming) -> Self {
        Self { inp }
    }
}

impl futures_util::Stream for IncomingStream {
    type Item = Result<Bytes, err::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let j = &mut self.get_mut().inp;
        let k = Pin::new(j);
        match hyper::body::Body::poll_frame(k, cx) {
            Ready(Some(x)) => match x {
                Ok(x) => {
                    if let Ok(x) = x.into_data() {
                        Ready(Some(Ok(x)))
                    } else {
                        Ready(Some(Ok(Bytes::new())))
                    }
                }
                Err(e) => Ready(Some(Err(e.into()))),
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
