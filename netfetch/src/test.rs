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
                port: 123,
                port_raw: 123,
                backend: "".into(),
                data_base_path: "".into(),
                cache_base_path: "".into(),
                listen: "".into(),
                ksprefix: "".into(),
                archiver_appliance: None,
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
                    run_map_pulse_task: false,
                    is_central_storage: false,
                    file_io_buffer_size: Default::default(),
                },
            },
            ix: 0,
        };
        let mut rx = super::ca::ca_connect_1(pairs, &node_config).await?;
        while let Some(item) = rx.next().await {
            info!("got next: {:?}", item);
        }
        Ok(())
    })
    .unwrap();
}
