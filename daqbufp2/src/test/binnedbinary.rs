use crate::err::ErrConv;
use crate::nodes::require_test_hosts_running;
use chrono::DateTime;
use chrono::Utc;
use disk::streamlog::Streamlog;
use err::Error;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http::StatusCode;
use httpclient::HttpBodyAsAsyncRead;
use hyper::Body;
use items_0::streamitem::StreamItem;
use items_0::subfr::SubFrId;
use netpod::log::*;
use netpod::query::BinnedQuery;
use netpod::query::CacheUsage;
use netpod::AggKind;
use netpod::AppendToUrl;
use netpod::Channel;
use netpod::Cluster;
use netpod::HostPort;
use netpod::NanoRange;
use netpod::PerfOpts;
use netpod::APP_OCTET;
use serde::de::DeserializeOwned;
use std::fmt;
use std::future::ready;
use streams::frames::inmem::InMemoryFrameAsyncReadStream;
use tokio::io::AsyncRead;
use url::Url;

#[test]
fn get_binned_binary() {
    taskrun::run(get_binned_binary_inner()).unwrap();
}

async fn get_binned_binary_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    if true {
        get_binned_channel::<i32>(
            "scalar-i32-be",
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T00:20:50.000Z",
            3,
            cluster,
            true,
            4,
        )
        .await?;
    }
    if true {
        return Ok(());
    };
    if true {
        get_binned_channel::<f64>(
            "wave-f64-be-n21",
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T00:20:30.000Z",
            2,
            cluster,
            true,
            2,
        )
        .await?;
    }
    if true {
        get_binned_channel::<u16>(
            "wave-u16-le-n77",
            "1970-01-01T01:11:00.000Z",
            "1970-01-01T01:35:00.000Z",
            7,
            cluster,
            true,
            24,
        )
        .await?;
    }
    if true {
        get_binned_channel::<u16>(
            "wave-u16-le-n77",
            "1970-01-01T01:42:00.000Z",
            "1970-01-01T03:55:00.000Z",
            2,
            cluster,
            true,
            3,
        )
        .await?;
    }
    Ok(())
}

async fn get_binned_channel<NTY>(
    channel_name: &str,
    beg_date: &str,
    end_date: &str,
    bin_count: u32,
    cluster: &Cluster,
    expect_range_complete: bool,
    expect_bin_count: u64,
) -> Result<BinnedResponse, Error>
where
    NTY: fmt::Debug + SubFrId + DeserializeOwned,
{
    let t1 = Utc::now();
    let agg_kind = AggKind::DimXBins1;
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let channel_backend = "testbackend";
    let perf_opts = PerfOpts::default();
    let channel = Channel {
        backend: channel_backend.into(),
        name: channel_name.into(),
        series: None,
    };
    let range = NanoRange::from_date_time(beg_date, end_date);
    let mut query = BinnedQuery::new(channel, range, bin_count, Some(agg_kind));
    query.set_cache_usage(CacheUsage::Ignore);
    query.set_buf_len_disk_io(1024 * 16);
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("get_binned_channel  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_OCTET)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
    }
    let s1 = HttpBodyAsAsyncRead::new(res);
    let s2 = InMemoryFrameAsyncReadStream::new(s1, perf_opts.inmem_bufcap);
    let res = consume_binned_response::<NTY, _>(s2).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    debug!("get_cached_0  DONE  bin_count {}  time {} ms", res.bin_count, ms);
    if !res.is_valid() {
        Err(Error::with_msg(format!("invalid response: {:?}", res)))
    } else if res.range_complete_count == 0 && expect_range_complete {
        Err(Error::with_msg(format!("expect range complete: {:?}", res)))
    } else if res.bin_count != expect_bin_count {
        Err(Error::with_msg(format!("bin count mismatch: {:?}", res)))
    } else {
        Ok(res)
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct BinnedResponse {
    bin_count: u64,
    err_item_count: u64,
    data_item_count: u64,
    bytes_read: u64,
    range_complete_count: u64,
    log_item_count: u64,
    #[allow(unused)]
    stats_item_count: u64,
}

impl BinnedResponse {
    pub fn new() -> Self {
        Self {
            bin_count: 0,
            err_item_count: 0,
            data_item_count: 0,
            bytes_read: 0,
            range_complete_count: 0,
            log_item_count: 0,
            stats_item_count: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        if self.range_complete_count > 1 {
            false
        } else {
            true
        }
    }
}

// TODO
async fn consume_binned_response<NTY, T>(inp: InMemoryFrameAsyncReadStream<T>) -> Result<BinnedResponse, Error>
where
    NTY: fmt::Debug + SubFrId + DeserializeOwned,
    T: AsyncRead + Unpin,
{
    let s1 = inp
        .map_err(|e| error!("TEST GOT ERROR {:?}", e))
        .filter_map(|item| {
            let g = match item {
                Ok(item) => match item {
                    StreamItem::Log(item) => {
                        Streamlog::emit(&item);
                        None
                    }
                    StreamItem::Stats(item) => {
                        // TODO collect somewhere
                        debug!("Stats: {:?}", item);
                        None
                    }
                    StreamItem::DataItem(_frame) => {
                        err::todo();
                        Some(Ok(()))
                    }
                },
                Err(e) => Some(Err(Error::with_msg(format!("WEIRD EMPTY ERROR {:?}", e)))),
            };
            ready(g)
        })
        .fold(BinnedResponse::new(), |a, _x| ready(a));
    let ret = s1.await;
    debug!("BinnedResponse: {:?}", ret);
    Ok(ret)
}
