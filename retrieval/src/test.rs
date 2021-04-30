use crate::spawn_test_hosts;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use disk::frame::inmem::InMemoryFrameAsyncReadStream;
use err::Error;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use hyper::Body;
use netpod::log::*;
use netpod::{Cluster, Database, Node};
use std::future::ready;
use tokio::io::AsyncRead;

fn test_cluster() -> Cluster {
    let nodes = (0..3)
        .into_iter()
        .map(|id| Node {
            id: format!("{:02}", id),
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 8360 + id as u16,
            port_raw: 8360 + id as u16 + 100,
            data_base_path: format!("../tmpdata/node{:02}", id).into(),
            ksprefix: "ks".into(),
            split: id,
        })
        .collect();
    Cluster {
        nodes: nodes,
        database: Database {
            name: "daqbuffer".into(),
            host: "localhost".into(),
            user: "daqbuffer".into(),
            pass: "daqbuffer".into(),
        },
    }
}

#[test]
fn get_binned() {
    taskrun::run(get_binned_0_inner()).unwrap();
}

async fn get_binned_0_inner() -> Result<(), Error> {
    let cluster = test_cluster();
    let _hosts = spawn_test_hosts(cluster.clone());
    get_binned_channel(
        "wave-f64-be-n21",
        "1970-01-01T00:20:10.000Z",
        "1970-01-01T00:20:51.000Z",
        4,
        &cluster,
    )
    .await?;
    get_binned_channel(
        "wave-u16-le-n77",
        "1970-01-01T01:11:00.000Z",
        "1970-01-01T02:12:00.000Z",
        4,
        &cluster,
    )
    .await?;
    get_binned_channel(
        "wave-u16-le-n77",
        "1970-01-01T01:42:00.000Z",
        "1970-01-01T03:55:00.000Z",
        2,
        &cluster,
    )
    .await?;
    Ok(())
}

async fn get_binned_channel<S>(
    channel_name: &str,
    beg_date: S,
    end_date: S,
    bin_count: u32,
    cluster: &Cluster,
) -> Result<(), Error>
where
    S: AsRef<str>,
{
    let t1 = Utc::now();
    let node0 = &cluster.nodes[0];
    let beg_date: DateTime<Utc> = beg_date.as_ref().parse()?;
    let end_date: DateTime<Utc> = end_date.as_ref().parse()?;
    let channel_backend = "back";
    let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
    let uri = format!(
        "http://{}:{}/api/1/binned?channel_backend={}&channel_name={}&bin_count={}&beg_date={}&end_date={}",
        node0.host,
        node0.port,
        channel_backend,
        channel_name,
        bin_count,
        beg_date.format(date_fmt),
        end_date.format(date_fmt),
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
    let res = consume_binned_response(s2).await?;
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    //let throughput = ntot / 1024 * 1000 / ms;
    info!("get_cached_0 DONE  bin_count {}  time {} ms", res.bin_count, ms);
    Ok(())
}

#[derive(Debug)]
pub struct BinnedResponse {
    bin_count: usize,
}

impl BinnedResponse {
    pub fn new() -> Self {
        Self { bin_count: 0 }
    }
}

async fn consume_binned_response<T>(inp: InMemoryFrameAsyncReadStream<T>) -> Result<BinnedResponse, Error>
where
    T: AsyncRead + Unpin,
{
    let s1 = inp
        .map_err(|e| error!("TEST GOT ERROR {:?}", e))
        .filter_map(|item| {
            let g = match item {
                Ok(frame) => {
                    type ExpectedType = disk::cache::BinnedBytesForHttpStreamFrame;
                    //info!("TEST GOT FRAME  len {}", frame.buf().len());
                    match bincode::deserialize::<ExpectedType>(frame.buf()) {
                        Ok(item) => match item {
                            Ok(item) => {
                                info!("TEST GOT ITEM {:?}", item);
                                Some(Ok(item))
                            }
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
                Err(e) => Some(Err(Error::with_msg(format!("WEIRD EMPTY ERROR {:?}", e)))),
            };
            ready(g)
        })
        .fold(Ok(BinnedResponse::new()), |a, k| {
            let g = match a {
                Ok(mut a) => match k {
                    Ok(k) => {
                        a.bin_count += k.ts1s.len();
                        Ok(a)
                    }
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            };
            ready(g)
        });
    let ret = s1.await;
    ret
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
