use crate::nodes::require_test_hosts_running;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use disk::agg::streams::{StatsItem, StreamItem};
use disk::binned::query::{BinnedQuery, CacheUsage};
use disk::binned::{MinMaxAvgBins, RangeCompletableItem, WithLen};
use disk::frame::inmem::InMemoryFrameAsyncReadStream;
use disk::frame::makeframe::{FrameType, SubFrId};
use disk::streamlog::Streamlog;
use disk::Sitemty;
use err::Error;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::{AggKind, Channel, Cluster, HostPort, NanoRange, PerfOpts};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::future::ready;
use tokio::io::AsyncRead;

pub mod binnedjson;
pub mod events;

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
    NTY: Debug + SubFrId + DeserializeOwned,
{
    let t1 = Utc::now();
    let agg_kind = AggKind::DimXBins1;
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let channel_backend = "testbackend";
    let perf_opts = PerfOpts { inmem_bufcap: 512 };
    let channel = Channel {
        backend: channel_backend.into(),
        name: channel_name.into(),
    };
    let range = NanoRange::from_date_time(beg_date, end_date);
    let mut query = BinnedQuery::new(channel, range, bin_count, agg_kind);
    query.set_cache_usage(CacheUsage::Ignore);
    let hp = HostPort::from_node(node0);
    let url = query.url(&hp);
    info!("get_binned_channel  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url)
        .header("Accept", "application/octet-stream")
        .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
    }
    let s1 = disk::cache::HttpBodyAsAsyncRead::new(res);
    let s2 = InMemoryFrameAsyncReadStream::new(s1, perf_opts.inmem_bufcap);
    let res = consume_binned_response::<NTY, _>(s2).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("get_cached_0  DONE  bin_count {}  time {} ms", res.bin_count, ms);
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

#[derive(Debug)]
pub struct BinnedResponse {
    bin_count: u64,
    err_item_count: u64,
    data_item_count: u64,
    bytes_read: u64,
    range_complete_count: u64,
    log_item_count: u64,
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

async fn consume_binned_response<NTY, T>(inp: InMemoryFrameAsyncReadStream<T>) -> Result<BinnedResponse, Error>
where
    NTY: Debug + SubFrId + DeserializeOwned,
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
                        info!("Stats: {:?}", item);
                        None
                    }
                    StreamItem::DataItem(frame) => {
                        if frame.tyid() != <Sitemty<MinMaxAvgBins<NTY>> as FrameType>::FRAME_TYPE_ID {
                            error!("test receives unexpected tyid {:x}", frame.tyid());
                        }
                        match bincode::deserialize::<Sitemty<MinMaxAvgBins<NTY>>>(frame.buf()) {
                            Ok(item) => match item {
                                Ok(item) => match item {
                                    StreamItem::Log(item) => {
                                        Streamlog::emit(&item);
                                        Some(Ok(StreamItem::Log(item)))
                                    }
                                    item => {
                                        info!("TEST GOT ITEM {:?}", item);
                                        Some(Ok(item))
                                    }
                                },
                                Err(e) => {
                                    error!("TEST GOT ERROR FRAME: {:?}", e);
                                    Some(Err(e))
                                }
                            },
                            Err(e) => {
                                error!("bincode error: {:?}", e);
                                Some(Err(e.into()))
                            }
                        }
                    }
                },
                Err(e) => Some(Err(Error::with_msg(format!("WEIRD EMPTY ERROR {:?}", e)))),
            };
            ready(g)
        })
        .fold(BinnedResponse::new(), |mut a, k| {
            let g = match k {
                Ok(StreamItem::Log(_item)) => {
                    a.log_item_count += 1;
                    a
                }
                Ok(StreamItem::Stats(item)) => match item {
                    StatsItem::EventDataReadStats(item) => {
                        a.bytes_read += item.parsed_bytes;
                        a
                    }
                },
                Ok(StreamItem::DataItem(item)) => match item {
                    RangeCompletableItem::RangeComplete => {
                        a.range_complete_count += 1;
                        a
                    }
                    RangeCompletableItem::Data(item) => {
                        a.data_item_count += 1;
                        a.bin_count += WithLen::len(&item) as u64;
                        a
                    }
                },
                Err(_e) => {
                    a.err_item_count += 1;
                    a
                }
            };
            ready(g)
        });
    let ret = s1.await;
    info!("BinnedResponse: {:?}", ret);
    Ok(ret)
}

#[test]
fn bufs() {
    use bytes::{Buf, BufMut};
    let mut buf = BytesMut::with_capacity(1024);
    assert!(buf.as_mut().len() == 0);
    buf.put_u32_le(123);
    assert!(buf.as_mut().len() == 4);
    let mut b2 = buf.split_to(4);
    assert!(b2.capacity() == 4);
    b2.advance(2);
    assert!(b2.capacity() == 2);
    b2.advance(2);
    assert!(b2.capacity() == 0);
    assert!(buf.capacity() == 1020);
    assert!(buf.remaining() == 0);
    assert!(buf.remaining_mut() >= 1020);
    assert!(buf.capacity() == 1020);
}
