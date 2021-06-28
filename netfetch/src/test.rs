use futures_util::StreamExt;
use netpod::log::*;
use netpod::{Cluster, Database, Node, NodeConfig, NodeConfigCached};
use std::collections::BTreeMap;
use std::iter::FromIterator;

#[test]
fn ca_connect_1() {
    taskrun::run(async {
        let it = vec![(String::new(), String::new())].into_iter();
        let pairs = BTreeMap::from_iter(it);
        let node_config = NodeConfigCached {
            node: Node {
                host: "".into(),
                bin_grain_kind: 0,
                port: 123,
                port_raw: 123,
                backend: "".into(),
                split: 0,
                data_base_path: "".into(),
                listen: "".into(),
                ksprefix: "".into(),
            },
            node_config: NodeConfig {
                name: "".into(),
                cluster: Cluster {
                    nodes: vec![],
                    database: Database {
                        host: "".into(),
                        name: "".into(),
                        user: "".into(),
                        pass: "".into(),
                    },
                },
            },
            ix: 0,
        };
        let mut rx = super::ca_connect_1(pairs, &node_config).await?;
        while let Some(item) = rx.next().await {
            info!("got next: {:?}", item);
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn zmtp_00() {
    taskrun::run(async {
        let it = vec![(String::new(), String::new())].into_iter();
        let _pairs = BTreeMap::from_iter(it);
        crate::zmtp::zmtp_00().await?;
        Ok(())
    })
    .unwrap();
}
