use bytes::Bytes;
use err::Error;
use err::PublicError;
use futures_util::pin_mut;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use hyper::body::HttpBody;
use hyper::Body;
use hyper::Method;
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::NodeConfigCached;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;
use url::Url;

pub trait ErrConv<T> {
    fn ec(self) -> Result<T, ::err::Error>;
}

pub trait Convable: ToString {}

impl<T, E: Convable> ErrConv<T> for Result<T, E> {
    fn ec(self) -> Result<T, ::err::Error> {
        match self {
            Ok(x) => Ok(x),
            Err(e) => Err(::err::Error::from_string(e.to_string())),
        }
    }
}

impl Convable for http::Error {}
impl Convable for hyper::Error {}

pub struct HttpResponse {
    pub head: http::response::Parts,
    pub body: Bytes,
}

pub async fn http_get(url: Url, accept: &str) -> Result<HttpResponse, Error> {
    let req = Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(header::ACCEPT, accept)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    let (head, body) = res.into_parts();
    debug!("http_get  head {head:?}");
    let body = hyper::body::to_bytes(body).await.ec()?;
    let ret = HttpResponse { head, body };
    Ok(ret)
}

pub async fn http_post(url: Url, accept: &str, body: String) -> Result<Bytes, Error> {
    let req = Request::builder()
        .method(http::Method::POST)
        .uri(url.to_string())
        .header(header::ACCEPT, accept)
        .body(Body::from(body))
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        let (head, body) = res.into_parts();
        let buf = hyper::body::to_bytes(body).await.ec()?;
        let s = String::from_utf8_lossy(&buf);
        return Err(Error::with_msg(format!(
            concat!(
                "Server error  {:?}\n",
                "---------------------- message from http body:\n",
                "{}\n",
                "---------------------- end of http body",
            ),
            head, s
        )));
    }
    let body = hyper::body::to_bytes(res.into_body()).await.ec()?;
    Ok(body)
}

// TODO move to a better fitting module:
pub struct HttpBodyAsAsyncRead {
    inp: Response<Body>,
    left: Bytes,
    rp: usize,
}

impl HttpBodyAsAsyncRead {
    pub fn new(inp: Response<Body>) -> Self {
        Self {
            inp,
            left: Bytes::new(),
            rp: 0,
        }
    }
}

impl AsyncRead for HttpBodyAsAsyncRead {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<io::Result<()>> {
        use Poll::*;
        if self.left.len() != 0 {
            let n1 = buf.remaining();
            let n2 = self.left.len() - self.rp;
            if n2 <= n1 {
                buf.put_slice(self.left[self.rp..].as_ref());
                self.left = Bytes::new();
                self.rp = 0;
                Ready(Ok(()))
            } else {
                buf.put_slice(self.left[self.rp..(self.rp + n2)].as_ref());
                self.rp += n2;
                Ready(Ok(()))
            }
        } else {
            let f = &mut self.inp;
            pin_mut!(f);
            match f.poll_data(cx) {
                Ready(Some(Ok(k))) => {
                    let n1 = buf.remaining();
                    if k.len() <= n1 {
                        buf.put_slice(k.as_ref());
                        Ready(Ok(()))
                    } else {
                        buf.put_slice(k[..n1].as_ref());
                        self.left = k;
                        self.rp = n1;
                        Ready(Ok(()))
                    }
                }
                Ready(Some(Err(e))) => Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    Error::with_msg(format!("Received by HttpBodyAsAsyncRead: {:?}", e)),
                ))),
                Ready(None) => Ready(Ok(())),
                Pending => Pending,
            }
        }
    }
}

pub async fn get_channel_config(
    q: &ChannelConfigQuery,
    node_config: &NodeConfigCached,
) -> Result<ChannelConfigResponse, Error> {
    let mut url = Url::parse(&format!(
        "http://{}:{}/api/4/channel/config",
        node_config.node.host, node_config.node.port
    ))?;
    q.append_to_url(&mut url);
    let req = hyper::Request::builder()
        .method(Method::GET)
        .uri(url.as_str())
        .body(Body::empty())
        .map_err(Error::from_string)?;
    let client = hyper::Client::new();
    let res = client
        .request(req)
        .await
        .map_err(|e| Error::with_msg(format!("get_channel_config request error: {e:?}")))?;
    if res.status().is_success() {
        let buf = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(|e| Error::with_msg(format!("can not read response: {e:?}")))?;
        let ret: ChannelConfigResponse = serde_json::from_slice(&buf)
            .map_err(|e| Error::with_msg(format!("can not parse the channel config response json: {e:?}")))?;
        Ok(ret)
    } else {
        let buf = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(|e| Error::with_msg(format!("can not read response: {e:?}")))?;
        match serde_json::from_slice::<PublicError>(&buf) {
            Ok(e) => Err(e.into()),
            Err(_) => Err(Error::with_msg(format!(
                "can not parse the http error body: {:?}",
                String::from_utf8_lossy(&buf)
            ))),
        }
    }
}
