use chrono::{DateTime, Utc};
use disk::frame::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_util::TryStreamExt;
use hyper::Body;
use netpod::log::*;

pub async fn get_binned(
    host: String,
    port: u16,
    channel_name: String,
    beg_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    bin_count: u32,
) -> Result<(), Error> {
    let t1 = Utc::now();
    let channel_backend = "NOBACKEND";
    let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
    let uri = format!(
        "http://{}:{}/api/1/binned?channel_backend={}&channel_name={}&beg_date={}&end_date={}&bin_count={}",
        host,
        port,
        channel_backend,
        channel_name,
        beg_date.format(date_fmt),
        end_date.format(date_fmt),
        bin_count,
    );
    info!("URI {:?}", uri);
    let req = hyper::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .body(Body::empty())?;
    info!("Request for {:?}", req);
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    info!("client response {:?}", res);
    //let (res_head, mut res_body) = res.into_parts();
    let s1 = disk::cache::HttpBodyAsAsyncRead::new(res);
    let s2 = InMemoryFrameAsyncReadStream::new(s1);
    use futures_util::StreamExt;
    use std::future::ready;
    let mut bin_count = 0;
    let s3 = s2
        .map_err(|e| error!("{:?}", e))
        .filter_map(|item| {
            let g = match item {
                Ok(frame) => {
                    type ExpectedType = disk::cache::BinnedBytesForHttpStreamFrame;
                    info!("frame  len {}", frame.buf().len());
                    match bincode::deserialize::<ExpectedType>(frame.buf()) {
                        Ok(item) => match item {
                            Ok(item) => {
                                info!("item: {:?}", item);
                                bin_count += 1;
                                Some(Ok(item))
                            }
                            Err(e) => {
                                error!("error frame: {:?}", e);
                                Some(Err(e))
                            }
                        },
                        Err(e) => {
                            error!("bincode error: {:?}", e);
                            Some(Err(e.into()))
                        }
                    }
                }
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
