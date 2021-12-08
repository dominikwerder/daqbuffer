pub mod client;
pub mod err;
pub mod nodes;
#[cfg(test)]
pub mod test;

use ::err::Error;
use futures_util::TryFutureExt;
use netpod::{Cluster, NodeConfig, NodeConfigCached, ProxyConfig};
use tokio::task::JoinHandle;

pub fn spawn_test_hosts(cluster: Cluster) -> Vec<JoinHandle<Result<(), Error>>> {
    let mut ret = vec![];
    for node in &cluster.nodes {
        let node_config = NodeConfig {
            cluster: cluster.clone(),
            name: format!("{}:{}", node.host, node.port),
        };
        let node_config: Result<NodeConfigCached, Error> = node_config.into();
        let node_config = node_config.unwrap();
        let h = tokio::spawn(httpret::host(node_config).map_err(Error::from));
        ret.push(h);
    }
    ret
}

pub async fn run_node(node_config: NodeConfigCached) -> Result<(), Error> {
    httpret::host(node_config).await?;
    Ok(())
}

pub async fn run_proxy(proxy_config: ProxyConfig) -> Result<(), Error> {
    httpret::proxy::proxy(proxy_config).await?;
    Ok(())
}
