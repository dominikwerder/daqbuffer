use netpod::{Cluster, Database, FileIoBufferSize, Node, NodeConfig, SfDatabuffer};
use tokio::net::TcpListener;

use super::*;

#[test]
fn raw_data_00() {
    //taskrun::run(disk::gen::gen_test_data()).unwrap();
    let fut = async {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let con = TcpStream::connect(lis.local_addr().unwrap()).await.unwrap();
        let (client, addr) = lis.accept().await.unwrap();
        let cfg = NodeConfigCached {
            node_config: NodeConfig {
                name: "node_name_dummy".into(),
                cluster: Cluster {
                    backend: "backend_dummy".into(),
                    nodes: vec![],
                    database: Database {
                        name: "".into(),
                        host: "".into(),
                        port: 5432,
                        user: "".into(),
                        pass: "".into(),
                    },
                    run_map_pulse_task: false,
                    is_central_storage: false,
                    file_io_buffer_size: FileIoBufferSize(1024 * 8),
                    scylla: None,
                    cache_scylla: None,
                },
            },
            node: Node {
                host: "empty".into(),
                listen: "listen_dummy".into(),
                port: 9090,
                port_raw: 9090,
                cache_base_path: "".into(),
                sf_databuffer: Some(SfDatabuffer {
                    data_base_path: "/home/dominik/daqbuffer-testdata/databuffer/node00".into(),
                    ksprefix: "ks".into(),
                    splits: None,
                }),
                archiver_appliance: None,
                channel_archiver: None,
                prometheus_api_bind: None,
            },
            ix: 0,
        };
        events_conn_handler(client, addr, cfg).await.unwrap();
        Ok(())
    };
    taskrun::run(fut).unwrap();
}
