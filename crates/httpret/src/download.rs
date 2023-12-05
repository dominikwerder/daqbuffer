use crate::body_empty;
use crate::err::Error;
use crate::response;
use crate::Requ;
use crate::RespFull;
use crate::StreamBody;
use bytes::Bytes;
use futures_util::Stream;
use futures_util::TryStreamExt;
use http::Method;
use http::Response;
use http::StatusCode;
use http_body_util::BodyExt;
use httpclient::httpclient::http_body_util;
use httpclient::RespBox;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::DiskIoTune;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use taskrun::tokio;
use url::Url;

#[derive(Clone, Debug)]
pub struct DownloadQuery {
    disk_io_tune: DiskIoTune,
}

impl FromUrl for DownloadQuery {
    fn from_url(url: &Url) -> Result<Self, ::err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &std::collections::BTreeMap<String, String>) -> Result<Self, err::Error> {
        let read_sys = pairs
            .get("ReadSys")
            .map(|x| x.as_str().into())
            .unwrap_or_else(|| netpod::ReadSys::default());
        let read_buffer_len = pairs
            .get("ReadBufferLen")
            .map(|x| x.parse().map_or(None, Some))
            .unwrap_or(None)
            .unwrap_or(1024 * 4);
        let read_queue_len = pairs
            .get("ReadQueueLen")
            .map(|x| x.parse().map_or(None, Some))
            .unwrap_or(None)
            .unwrap_or(8);
        let disk_io_tune = DiskIoTune {
            read_sys,
            read_buffer_len,
            read_queue_len,
        };
        let ret = Self { disk_io_tune };
        Ok(ret)
    }
}

pub struct DownloadHandler {}

impl DownloadHandler {
    pub fn path_prefix() -> &'static str {
        "/api/4/test/download/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<RespBox, Error> {
        if req.method() == Method::GET {
            self.get(req, node_config).await
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    pub async fn get(&self, req: Requ, ncc: &NodeConfigCached) -> Result<RespBox, Error> {
        let (head, _body) = req.into_parts();
        let p2 = &head.uri.path()[Self::path_prefix().len()..];
        let base = match &ncc.node.sf_databuffer {
            Some(k) => k.data_base_path.clone(),
            None => "/UNDEFINED".into(),
        };
        let url = url::Url::parse(&format!("http://dummy{}", head.uri))?;
        let query = DownloadQuery::from_url(&url)?;
        // TODO wrap file operation to return a better error.
        let pp = base.join(p2);
        info!("Try to open {pp:?}");
        let file = tokio::fs::OpenOptions::new().read(true).open(&pp).await?;
        let stream =
            disk::file_content_stream(pp, file, query.disk_io_tune.clone(), "download").map_ok(|x| x.into_buf());

        use futures_util::StreamExt;
        use hyper::body::Frame;
        let stream = stream.map(|item| item.map(|x| Frame::data(x.freeze())));
        let body = httpclient::httpclient::http_body_util::StreamBody::new(stream);
        let body = BodyExt::boxed(body);
        // let body = http_body_util::combinators::BoxBody::new(body);
        // let body: StreamBody = Box::pin(body);
        // let body: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, err::Error>>>> = Box::pin(body);
        let res = response(StatusCode::OK).body(body)?;
        Ok(res)
    }
}
