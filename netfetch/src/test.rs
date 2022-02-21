use err::Error;
use futures_util::StreamExt;
use netpod::{log::*, SfDatabuffer};
use netpod::{Cluster, Database, Node, NodeConfig, NodeConfigCached};
use std::collections::BTreeMap;
use std::iter::FromIterator;
use std::time::Duration;

#[test]
fn ca_connect_1() {
    let fut = async {
        let it = vec![(String::new(), String::new())].into_iter();
        let pairs = BTreeMap::from_iter(it);
        let node_config = NodeConfigCached {
            node: Node {
                host: "".into(),
                port: 123,
                port_raw: 123,
                cache_base_path: "".into(),
                listen: "".into(),
                sf_databuffer: Some(SfDatabuffer {
                    data_base_path: "".into(),
                    ksprefix: "".into(),
                    splits: None,
                }),
                archiver_appliance: None,
                channel_archiver: None,
            },
            node_config: NodeConfig {
                name: "".into(),
                cluster: Cluster {
                    backend: "".into(),
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
            debug!("got next: {:?}", item);
        }
        Ok::<_, Error>(())
    };
    let fut = async move {
        let ret = tokio::time::timeout(Duration::from_millis(4000), fut)
            .await
            .map_err(Error::from_string)??;
        Ok(ret)
    };
    taskrun::run(fut).unwrap();
}
