use err::Error;
use netpod::{Cluster, NodeConfig};
use std::sync::Arc;
use tokio::task::JoinHandle;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub mod cli;
#[cfg(test)]
pub mod test;

pub fn spawn_test_hosts(cluster: Arc<Cluster>) -> Vec<JoinHandle<Result<(), Error>>> {
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
