use err::Error;
use netpod::{timeunits::*, Channel, ChannelConfig, Cluster, Node, NodeConfig, ScalarType, Shape};
use std::sync::Arc;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub fn main() {
    match taskrun::run(go()) {
        Ok(k) => {
            info!("{:?}", k);
        }
        Err(k) => {
            error!("{:?}", k);
        }
    }
}

async fn go() -> Result<(), Error> {
    use clap::Clap;
    use retrieval::cli::{Opts, SubCmd};
    let opts = Opts::parse();
    match opts.subcmd {
        SubCmd::Retrieval(_subcmd) => {
            trace!("testout");
            info!("testout");
            error!("testout");
        }
    }
    Ok(())
}

#[test]
fn simple_fetch() {
    taskrun::run(async {
        let t1 = chrono::Utc::now();
        let node = Node {
            id: 0,
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 8360,
            port_raw: 8360 + 100,
            data_base_path: err::todoval(),
            ksprefix: "daq_swissfel".into(),
            split: 0,
        };
        let node = Arc::new(node);
        let query = netpod::AggQuerySingleChannel {
            channel_config: ChannelConfig {
                channel: Channel {
                    backend: "sf-databuffer".into(),
                    name: "S10BC01-DBAM070:BAM_CH1_NORM".into(),
                },
                keyspace: 3,
                time_bin_size: DAY,
                array: true,
                scalar_type: ScalarType::F64,
                shape: Shape::Wave(err::todoval()),
                big_endian: true,
                compression: true,
            },
            timebin: 18720,
            tb_file_count: 1,
            buffer_size: 1024 * 8,
        };
        let cluster = Cluster { nodes: vec![node] };
        let cluster = Arc::new(cluster);
        let node_config = NodeConfig {
            node: cluster.nodes[0].clone(),
            cluster: cluster,
        };
        let node_config = Arc::new(node_config);
        let query_string = serde_json::to_string(&query).unwrap();
        let host = tokio::spawn(httpret::host(node_config));
        let req = hyper::Request::builder()
            .method(http::Method::POST)
            .uri("http://localhost:8360/api/1/parsed_raw")
            .body(query_string.into())?;
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
            "total download {} MB   throughput {:5} kB/s",
            ntot / 1024 / 1024,
            throughput
        );
        drop(host);
        //Err::<(), _>(format!("test error").into())
        Ok(())
    })
    .unwrap();
}
