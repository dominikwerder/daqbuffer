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
use netpod::log::*;
use netpod::query::PlainEventsQuery;
use netpod::AppendToUrl;
use netpod::Channel;
use netpod::Cluster;
use netpod::HostPort;
use netpod::NanoRange;
use netpod::PerfOpts;
use netpod::APP_JSON;
use netpod::APP_OCTET;
use serde_json::Value as JsonValue;
use std::fmt::Debug;
use std::future::ready;
use streams::frames::inmem::InMemoryFrameAsyncReadStream;
use tokio::io::AsyncRead;
use url::Url;

fn ch_adhoc(name: &str) -> Channel {
    Channel {
        series: None,
        backend: "test-disk-databuffer".into(),
        name: name.into(),
    }
}

pub fn ch_gen(name: &str) -> Channel {
    Channel {
        series: None,
        backend: "test-disk-databuffer".into(),
        name: name.into(),
    }
}

// TODO OFFENDING TEST add actual checks on result
async fn get_plain_events_binary_0_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    if true {
        get_plain_events_binary(
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

#[test]
fn get_plain_events_binary_0() {
    taskrun::run(get_plain_events_binary_0_inner()).unwrap();
}

async fn get_plain_events_binary(
    channel_name: &str,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
    _expect_range_complete: bool,
    _expect_event_count: u64,
) -> Result<EventsResponse, Error> {
    let t1 = Utc::now();
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
    let query = PlainEventsQuery::new(channel, range).for_time_weighted_scalar();
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/events", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    debug!("get_plain_events_binary  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_OCTET)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("client response {res:?}");
        return Err(format!("get_plain_events_binary  client response {res:?}").into());
    }
    let s1 = HttpBodyAsAsyncRead::new(res);
    let s2 = InMemoryFrameAsyncReadStream::new(s1, perf_opts.inmem_bufcap);
    let res = consume_plain_events_binary(s2).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("get_plain_events_binary  time {} ms", ms);
    if !res.is_valid() {
        Ok(res)
    } else {
        Ok(res)
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct EventsResponse {
    event_count: u64,
    err_item_count: u64,
    data_item_count: u64,
    bytes_read: u64,
    range_complete_count: u64,
    log_item_count: u64,
    #[allow(unused)]
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

async fn consume_plain_events_binary<T>(inp: InMemoryFrameAsyncReadStream<T>) -> Result<EventsResponse, Error>
where
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
        .fold(EventsResponse::new(), |a, _x| ready(a));
    let ret = s1.await;
    debug!("result: {:?}", ret);
    Ok(ret)
}

async fn get_plain_events_json_0_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_plain_events_json(
        ch_gen("scalar-i32-be"),
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
fn get_plain_events_json_0() {
    taskrun::run(get_plain_events_json_0_inner()).unwrap();
}

async fn get_plain_events_json_1_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_plain_events_json(
        ch_gen("wave-f64-be-n21"),
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

async fn get_plain_events_json_2_inner() -> Result<(), Error> {
    let rh = require_test_hosts_running()?;
    let cluster = &rh.cluster;
    get_plain_events_json(
        ch_adhoc("inmem-d0-i32"),
        "1970-01-01T00:20:04.000Z",
        "1970-01-01T00:20:10.000Z",
        cluster,
        true,
        4,
    )
    .await?;
    Ok(())
}

#[test]
fn get_plain_events_json_2() {
    taskrun::run(get_plain_events_json_2_inner()).unwrap();
}

// TODO improve by a more information-rich return type.
pub async fn get_plain_events_json(
    channel: Channel,
    beg_date: &str,
    end_date: &str,
    cluster: &Cluster,
    _expect_range_complete: bool,
    _expect_event_count: u64,
) -> Result<JsonValue, Error> {
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.parse()?;
    let end_date: DateTime<Utc> = end_date.parse()?;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let query = PlainEventsQuery::new(channel, range);
    let hp = HostPort::from_node(node0);
    let mut url = Url::parse(&format!("http://{}:{}/api/4/events", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    info!("get_plain_events  get {}", url);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_JSON)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;

    trace!("Response {res:?}");

    if res.status() != StatusCode::OK {
        error!("client response {:?}", res);
    }
    let buf = hyper::body::to_bytes(res.into_body()).await.ec()?;
    let s = String::from_utf8_lossy(&buf);
    let res: JsonValue = serde_json::from_str(&s)?;

    eprintln!("res {res:?}");

    // TODO assert more
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    // TODO add timeout
    debug!("time {} ms", ms);
    Ok(res)
}
