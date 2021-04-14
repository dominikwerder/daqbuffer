#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use tokio::task::JoinHandle;
use netpod::{Node, Cluster, NodeConfig};
use hyper::Body;
use std::sync::Arc;

pub mod cli;

#[test]
fn get_cached_0() {
    taskrun::run(get_cached_0_inner()).unwrap();
}

#[cfg(test)]
async fn get_cached_0_inner() -> Result<(), Error> {
    let t1 = chrono::Utc::now();
    let cluster = Arc::new(test_cluster());
    let node0 = &cluster.nodes[0];
    let hosts = spawn_test_hosts(cluster.clone());
    let req = hyper::Request::builder()
    .method(http::Method::GET)
    .uri(format!("http://{}:{}/api/1/binned?channel_backend=testbackend&channel_keyspace=3&channel_name=wave1&bin_count=4&beg_date=1970-01-01T00:00:10.000Z&end_date=1970-01-01T00:00:51.000Z", node0.host, node0.port))
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
    info!("get_cached_0 DONE  total download {} MB   throughput {:5} kB/s", ntot / 1024 / 1024, throughput);
    //Err::<(), _>(format!("test error").into())
    Ok(())
}


fn test_cluster() -> Cluster {
    let nodes = (0..1).into_iter().map(|id| {
        let node = Node {
            id,
            host: "localhost".into(),
            port: 8360 + id as u16,
            data_base_path: format!("../tmpdata/node{:02}", id).into(),
            ksprefix: "ks".into(),
            split: 0,
        };
        Arc::new(node)
    })
    .collect();
    Cluster {
        nodes: nodes,
    }
}

fn spawn_test_hosts(cluster: Arc<Cluster>) -> Vec<JoinHandle<Result<(), Error>>> {
    let mut ret = vec![];
    for node in &cluster.nodes {
        let node_config = NodeConfig {
            cluster: cluster.clone(),
            node: node.clone(),
        };
        let h = tokio::spawn(httpret::host(Arc::new(node_config)));
        ret.push(h);
    }
    ret
}
