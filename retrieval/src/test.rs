use crate::spawn_test_hosts;
use bytes::BytesMut;
use chrono::Utc;
use err::Error;
use futures_util::TryStreamExt;
use hyper::Body;
use netpod::{Cluster, Node};
use std::sync::Arc;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

fn test_cluster() -> Cluster {
    let nodes = (0..1)
        .into_iter()
        .map(|id| {
            let node = Node {
                id,
                host: "localhost".into(),
                listen: "0.0.0.0".into(),
                port: 8360 + id as u16,
                port_raw: 8360 + id as u16 + 100,
                data_base_path: format!("../tmpdata/node{:02}", id).into(),
                ksprefix: "ks".into(),
                split: 0,
            };
            Arc::new(node)
        })
        .collect();
    Cluster { nodes: nodes }
}

#[test]
fn get_cached_0() {
    taskrun::run(get_cached_0_inner()).unwrap();
}

async fn get_cached_0_inner() -> Result<(), Error> {
    let t1 = chrono::Utc::now();
    let cluster = Arc::new(test_cluster());
    let node0 = &cluster.nodes[0];
    let hosts = spawn_test_hosts(cluster.clone());
    let beg_date: chrono::DateTime<Utc> = "1970-01-01T00:00:10.000Z".parse()?;
    let end_date: chrono::DateTime<Utc> = "1970-01-01T00:00:51.000Z".parse()?;
    let channel_backend = "back";
    let channel_name = "wave1";
    let date_fmt = "%Y-%m-%dT%H:%M:%S.%3fZ";
    let uri = format!(
        "http://{}:{}/api/1/binned?channel_backend={}&channel_name={}&bin_count=4&beg_date={}&end_date={}",
        node0.host,
        node0.port,
        channel_backend,
        channel_name,
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
    let s2 = disk::raw::InMemoryFrameAsyncReadStream::new(s1);
    /*use hyper::body::HttpBody;
    loop {
        match res_body.data().await {
            Some(Ok(k)) => {
                info!("packet..  len {}", k.len());
                ntot += k.len() as u64;
            }
            Some(Err(e)) => {
                error!("{:?}", e);
            }
            None => {
                info!("response stream exhausted");
                break;
            }
        }
    }*/
    use futures_util::StreamExt;
    use std::future::ready;
    let mut bin_count = 0;
    let s3 = s2
        .map_err(|e| error!("TEST GOT ERROR {:?}", e))
        .filter_map(|item| {
            let g = match item {
                Ok(frame) => {
                    type ExpectedType = disk::cache::BinnedBytesForHttpStreamFrame;
                    info!("TEST GOT FRAME  len {}", frame.buf().len());
                    match bincode::deserialize::<ExpectedType>(frame.buf()) {
                        Ok(item) => match item {
                            Ok(item) => {
                                info!("TEST GOT ITEM");
                                bin_count += 1;
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
    drop(hosts);
    //Err::<(), _>(format!("test error").into())
    Ok(())
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
