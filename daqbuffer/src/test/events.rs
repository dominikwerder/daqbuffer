use crate::nodes::require_test_hosts_running;
use chrono::{DateTime, Utc};
use disk::agg::streams::{StatsItem, StreamItem};
use disk::binned::{NumOps, RangeCompletableItem, WithLen};
use disk::decode::EventValues;
use disk::events::{PlainEventsBinaryQuery, PlainEventsJsonQuery};
use disk::frame::inmem::InMemoryFrameAsyncReadStream;
use disk::frame::makeframe::FrameType;
use disk::streamlog::Streamlog;
use disk::Sitemty;
use err::Error;
use futures_util::{StreamExt, TryStreamExt};
use http::StatusCode;
use hyper::Body;
use netpod::log::*;
use netpod::{Channel, Cluster, HostPort, NanoRange, PerfOpts};
use serde_json::Value as JsonValue;
use std::fmt::Debug;
use std::future::ready;
use tokio::io::AsyncRead;

#[test]
fn get_plain_events_binary_0() {
    taskrun::run(get_plain_events_binary_0_inner()).unwrap();
}

async fn get_plain_events_binary_0_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    if true {
        get_plain_events_binary::<i32>(
            "scalar-i32-be",
            "1970-01-01T00:20:10.000Z",
            "1970-01-01T00:20:50.000Z",
            cluster,
            true,
            4,
        )
        .await?;
    }
    Ok(())
}

async fn get_plain_events_binary<NTY>(
    channel_name: &str,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
    _expect_range_complete: bool,
    _expect_event_count: u64,
) -> Result<EventsResponse, Error>
where
    NTY: NumOps,
{
    let t1 = Utc::now();
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
    let query = PlainEventsBinaryQuery::new(channel, range);
    let hp = HostPort::from_node(node0);
    let url = query.url(&hp);
    info!("get_plain_events  get {}", url);
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
    let res = consume_plain_events_binary::<NTY, _>(s2).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("time {} ms", ms);
    if !res.is_valid() {
        Ok(res)
    } else {
        Ok(res)
    }
}

#[derive(Debug)]
pub struct EventsResponse {
    event_count: u64,
    err_item_count: u64,
    data_item_count: u64,
    bytes_read: u64,
    range_complete_count: u64,
    log_item_count: u64,
    stats_item_count: u64,
}

impl EventsResponse {
    pub fn new() -> Self {
        Self {
            event_count: 0,
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

async fn consume_plain_events_binary<NTY, T>(inp: InMemoryFrameAsyncReadStream<T>) -> Result<EventsResponse, Error>
where
    NTY: NumOps,
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
                        if frame.tyid() != <Sitemty<EventValues<NTY>> as FrameType>::FRAME_TYPE_ID {
                            error!("test receives unexpected tyid {:x}", frame.tyid());
                            None
                        } else {
                            match bincode::deserialize::<Sitemty<EventValues<NTY>>>(frame.buf()) {
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
                    }
                },
                Err(e) => Some(Err(Error::with_msg(format!("WEIRD EMPTY ERROR {:?}", e)))),
            };
            ready(g)
        })
        .fold(EventsResponse::new(), |mut a, k| {
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
                        a.event_count += WithLen::len(&item) as u64;
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
    info!("result: {:?}", ret);
    Ok(ret)
}

#[test]
fn get_plain_events_json_0() {
    taskrun::run(get_plain_events_json_0_inner()).unwrap();
}

async fn get_plain_events_json_0_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_plain_events_json(
        "scalar-i32-be",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T00:20:12.000Z",
        cluster,
        true,
        4,
    )
    .await?;
    Ok(())
}

#[test]
fn get_plain_events_json_1() {
    taskrun::run(get_plain_events_json_1_inner()).unwrap();
}

async fn get_plain_events_json_1_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_plain_events_json(
        "wave-f64-be-n21",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T00:20:12.000Z",
        cluster,
        true,
        4,
    )
    .await?;
    Ok(())
}

async fn get_plain_events_json(
    channel_name: &str,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
    _expect_range_complete: bool,
    _expect_event_count: u64,
) -> Result<(), Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let channel_backend = "testbackend";
    let channel = Channel {
        backend: channel_backend.into(),
        name: channel_name.into(),
    };
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsJsonQuery::new(channel, range);
    let hp = HostPort::from_node(node0);
    let url = query.url(&hp);
    info!("get_plain_events  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url)
        .header("Accept", "application/json")
        .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
    }
    let buf = hyper::body::to_bytes(res.into_body()).await?;
    let s = String::from_utf8_lossy(&buf);
    let res: JsonValue = serde_json::from_str(&s)?;
    info!("GOT: {}", serde_json::to_string_pretty(&res)?);
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("time {} ms", ms);
    Ok(())
}
