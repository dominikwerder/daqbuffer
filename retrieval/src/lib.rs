use err::Error;
use netpod::{Cluster, Node, NodeConfig};
use tokio::task::JoinHandle;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub mod cli;
pub mod client;
#[cfg(test)]
pub mod test;

pub fn spawn_test_hosts(cluster: Cluster) -> Vec<JoinHandle<Result<(), Error>>> {
    let mut ret = vec![];
    for node in &cluster.nodes {
        let node_config = NodeConfig {
            cluster: cluster.clone(),
            nodeid: node.id.clone(),
        };
        let h = tokio::spawn(httpret::host(node_config, node.clone()));
        ret.push(h);
    }
    ret
}

pub async fn run_node(node_config: NodeConfig, node: Node) -> Result<(), Error> {
    httpret::host(node_config, node).await?;
    Ok(())
}
