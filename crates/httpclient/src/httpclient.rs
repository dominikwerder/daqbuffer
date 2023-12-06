pub use http_body_util;
pub use http_body_util::Full;
pub use hyper_util;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use http_body::Frame;
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Incoming;
use hyper::client::conn::http2::SendRequest;
use hyper::Method;
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::NodeConfigCached;
use netpod::APP_JSON;
use serde::Serialize;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::net::TcpStream;
use url::Url;

pub type BodyBox = BoxBody<Bytes, BodyError>;
pub type RespBox = Response<BodyBox>;
pub type Requ = Request<Incoming>;
pub type RespFull = Response<Full<Bytes>>;

// TODO rename: too similar.
pub type StreamBody = http_body_util::StreamBody<Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, BodyError>> + Send>>>;
pub type StreamResponse = Response<StreamBody>;

fn _assert1() {
    let body: Full<Bytes> = todoval();
    let _: &dyn Body<Data = _, Error = _> = &body;
}

fn _assert2() {
    let stream: Pin<Box<dyn futures_util::Stream<Item = Result<Frame<Bytes>, BodyError>>>> = todoval();
    let body = http_body_util::StreamBody::new(stream);
    let _: &dyn Body<Data = _, Error = _> = &body;
}

#[allow(unused)]
fn todoval<T>() -> T {
    todo!()
}

pub fn body_empty() -> StreamBody {
    // Full::new(Bytes::new()).map_err(Into::into).boxed()
    let fr = Frame::data(Bytes::new());
    let stream = futures_util::stream::iter([Ok(fr)]);
    http_body_util::StreamBody::new(Box::pin(stream))
}

pub fn body_string<S: ToString>(body: S) -> StreamBody {
    // Full::new(Bytes::from(body.to_string())).map_err(Into::into).boxed()
    let fr = Frame::data(Bytes::from(body.to_string()));
    let stream = futures_util::stream::iter([Ok(fr)]);
    http_body_util::StreamBody::new(Box::pin(stream))
}

pub fn body_bytes<D: Into<Bytes>>(body: D) -> StreamBody {
    let fr = Frame::data(body.into());
    let stream = futures_util::stream::iter([Ok(fr)]);
    http_body_util::StreamBody::new(Box::pin(stream))
}

pub trait IntoBody {
    fn into_body(self) -> StreamBody;
}

pub struct StringBody {
    body: String,
}

impl<S: ToString> From<S> for StringBody {
    fn from(value: S) -> Self {
        Self {
            body: value.to_string(),
        }
    }
}

impl IntoBody for StringBody {
    fn into_body(self) -> StreamBody {
        let fr = Frame::data(Bytes::from(self.body.as_bytes().to_vec()));
        let stream = futures_util::stream::iter([Ok(fr)]);
        http_body_util::StreamBody::new(Box::pin(stream))
    }
}

pub struct ToJsonBody {
    body: Vec<u8>,
}

impl<S: Serialize> From<&S> for ToJsonBody {
    fn from(value: &S) -> Self {
        Self {
            body: serde_json::to_vec(value).unwrap_or(Vec::new()),
        }
    }
}

impl IntoBody for ToJsonBody {
    fn into_body(self) -> StreamBody {
        let fr = Frame::data(Bytes::from(self.body));
        let stream = futures_util::stream::iter([Ok(fr)]);
        http_body_util::StreamBody::new(Box::pin(stream))
    }
}

pub fn body_stream<S, I, E>(stream: S) -> StreamBody
where
    S: Stream<Item = Result<I, E>> + Send + 'static,
    I: Into<Bytes>,
    E: fmt::Display,
{
    let stream = stream.map(|x| match x {
        Ok(x) => Ok(Frame::data(x.into())),
        Err(_e) => Err(BodyError::Bad),
    });
    StreamBody::new(Box::pin(stream))
}

pub struct StreamIncoming {
    inp: http_body_util::BodyStream<Incoming>,
}

impl StreamIncoming {
    pub fn new(inp: Incoming) -> Self {
        Self {
            inp: http_body_util::BodyStream::new(inp),
        }
    }
}

impl Stream for StreamIncoming {
    type Item = Result<Bytes, BodyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(x))) => {
                if x.is_data() {
                    Ready(Some(Ok(x.into_data().unwrap())))
                } else {
                    Ready(Some(Ok(Bytes::new())))
                }
            }
            Ready(Some(Err(e))) => {
                error!("{e}");
                Ready(Some(Err(BodyError::Bad)))
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

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
    fn from(_: std::io::Error) -> Self {
        Self::IO
    }
}

impl From<http::Error> for Error {
    fn from(_: http::Error) -> Self {
        Self::Http
    }
}

impl From<hyper::Error> for Error {
    fn from(_: hyper::Error) -> Self {
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
        .body(body_empty())?;
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    let (head, mut body) = res.into_parts();
    debug!("http_get  head {head:?}");
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
    let req = Request::builder()
        .method(http::Method::POST)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| Error::BadUrl)?)
        .header(header::CONTENT_TYPE, APP_JSON)
        .header(header::ACCEPT, accept)
        .body(body_string(body))?;
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        let (_head, body) = res.into_parts();
        let buf = read_body_bytes(body).await?;
        let s = String::from_utf8_lossy(&buf);
        error!("{s}");
        // TODO return error
        return Err(Error::Http);
    }
    let (head, mut body) = res.into_parts();
    debug!("http_get  head {head:?}");
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

pub async fn connect_client(uri: &http::Uri) -> Result<SendRequest<StreamBody>, Error> {
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
