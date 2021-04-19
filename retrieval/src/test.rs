use crate::spawn_test_hosts;
use err::Error;
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
    let req = hyper::Request::builder()
    .method(http::Method::GET)
    .uri(format!(
        "http://{}:{}/api/1/binned?channel_backend=testbackend&channel_keyspace=3&channel_name=wave1&bin_count=4&beg_date=1970-01-01T00:00:10.000Z&end_date=1970-01-01T00:00:51.000Z",
        node0.host, node0.port
    ))
    .body(Body::empty())?;
    let client = hyper::Client::new();
    let res = client.request(req).await?;
    info!("client response {:?}", res);
    let mut res_body = res.into_body();
    use hyper::body::HttpBody;
    let mut ntot = 0 as u64;
    loop {
        match res_body.data().await {
            Some(Ok(k)) => {
                //info!("packet..  len {}", k.len());
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
    }
    let t2 = chrono::Utc::now();
    let ms = t2.signed_duration_since(t1).num_milliseconds() as u64;
    let throughput = ntot / 1024 * 1000 / ms;
    info!(
        "get_cached_0 DONE  total download {} MB   throughput {:5} kB/s",
        ntot / 1024 / 1024,
        throughput
    );
    drop(hosts);
    //Err::<(), _>(format!("test error").into())
    Ok(())
}
