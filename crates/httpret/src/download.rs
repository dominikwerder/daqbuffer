use crate::err::Error;
use crate::response;
use futures_util::TryStreamExt;
use http::Method;
use http::StatusCode;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::DiskIoTune;
use netpod::FromUrl;
use netpod::NodeConfigCached;
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
            .map(|x| x as &str)
            .unwrap_or("TokioAsyncRead")
            .into();
        let read_buffer_len = pairs
            .get("ReadBufferLen")
            .map(|x| x as &str)
            .unwrap_or("xx")
            .parse()
            .unwrap_or(1024 * 4);
        let read_queue_len = pairs
            .get("ReadQueueLen")
            .map(|x| x as &str)
            .unwrap_or("xx")
            .parse()
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

    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn get(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        let (head, _body) = req.into_parts();
        let p2 = &head.uri.path()[Self::path_prefix().len()..];
        let base = match &node_config.node.sf_databuffer {
            Some(k) => k.data_base_path.clone(),
            None => "/UNDEFINED".into(),
        };
        let url = url::Url::parse(&format!("http://dummy{}", head.uri))?;
        let query = DownloadQuery::from_url(&url)?;
        // TODO wrap file operation to return a better error.
        let pp = base.join(p2);
        info!("Try to open {pp:?}");
        let file = tokio::fs::OpenOptions::new().read(true).open(&pp).await?;
        let s = disk::file_content_stream(pp, file, query.disk_io_tune.clone(), "download").map_ok(|x| x.into_buf());
        Ok(response(StatusCode::OK).body(Body::wrap_stream(s))?)
    }

    pub async fn handle(&self, req: Request<Body>, node_config: &NodeConfigCached) -> Result<Response<Body>, Error> {
        if req.method() == Method::GET {
            self.get(req, node_config).await
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}
