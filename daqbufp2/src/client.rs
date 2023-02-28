use crate::err::ErrConv;
use chrono::DateTime;
use chrono::Utc;
use disk::streamlog::Streamlog;
use err::Error;
use futures_util::TryStreamExt;
use http::StatusCode;
use httpclient::HttpBodyAsAsyncRead;
use hyper::Body;
use items_0::streamitem::StreamItem;
use netpod::log::*;
use netpod::query::BinnedQuery;
use netpod::query::CacheUsage;
use netpod::AggKind;
use netpod::AppendToUrl;
use netpod::ByteSize;
use netpod::Channel;
use netpod::HostPort;
use netpod::NanoRange;
use netpod::PerfOpts;
use netpod::APP_OCTET;
use streams::frames::inmem::InMemoryFrameAsyncReadStream;
use url::Url;

pub async fn status(host: String, port: u16) -> Result<(), Error> {
    let t1 = Utc::now();
    let uri = format!("http://{}:{}/api/4/node_status", host, port,);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .body(Body::empty())
        .ec()?;
    let client = hyper::Client::new();
    let res = client.request(req).await.ec()?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        return Err(Error::with_msg(format!("Server error  {:?}", res)));
    }
    let body = hyper::body::to_bytes(res.into_body()).await.ec()?;
    let res = String::from_utf8(body.to_vec())?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    info!("node_status DONE  duration: {} ms", ms);
    println!("{}", res);
    Ok(())
}

pub async fn get_binned(
    host: String,
    port: u16,
    channel_backend: String,
    channel_name: String,
    beg_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    bin_count: u32,
    cache_usage: CacheUsage,
    disk_stats_every_kb: u32,
) -> Result<(), Error> {
    info!("-------   get_binned  client");
    info!("channel {}", channel_name);
    info!("beg  {}", beg_date);
    info!("end  {}", end_date);
    info!("-------");
    let t1 = Utc::now();
    let channel = Channel {
        backend: channel_backend.clone(),
        name: channel_name.into(),
        series: None,
    };
    let agg_kind = AggKind::DimXBins1;
    let range = NanoRange::from_date_time(beg_date, end_date);
    let mut query = BinnedQuery::new(channel, range, bin_count, Some(agg_kind));
    query.set_cache_usage(cache_usage);
    query.set_disk_stats_every(ByteSize(1024 * disk_stats_every_kb));
    let hp = HostPort { host: host, port: port };
    let mut url = Url::parse(&format!("http://{}:{}/api/4/binned", hp.host, hp.port))?;
    query.append_to_url(&mut url);
    let url = url;
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(http::header::ACCEPT, APP_OCTET)
        .body(Body::empty())
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
    let perf_opts = PerfOpts::default();
    let s1 = HttpBodyAsAsyncRead::new(res);
    let s2 = InMemoryFrameAsyncReadStream::new(s1, perf_opts.inmem_bufcap);
    use futures_util::StreamExt;
    use std::future::ready;
    let s3 = s2
        .map_err(|e| error!("get_binned  {:?}", e))
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
                    StreamItem::DataItem(_frame) => {
                        // TODO
                        // The expected type nowadays depends on the channel and agg-kind.
                        err::todo();
                        Some(Ok(()))
                    }
                },
                Err(e) => Some(Err(Error::with_msg(format!("{:?}", e)))),
            };
            ready(g)
        })
        .for_each(|_| ready(()));
    s3.await;
    let t2 = chrono::Utc::now();
    let ntot = 0;
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    let throughput = ntot / 1024 * 1000 / ms;
    info!(
        "get_cached_0 DONE  total download {} MB   throughput {:5} kB/s  bin_count {}",
        ntot / 1024 / 1024,
        throughput,
        bin_count,
    );
    Ok(())
}
