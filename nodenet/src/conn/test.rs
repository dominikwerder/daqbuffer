use super::*;
use disk::eventchunker::EventFull;
use items::frame::make_frame;
use items::Sitemty;
use netpod::timeunits::SEC;
use netpod::{Channel, Cluster, Database, DiskIoTune, FileIoBufferSize, NanoRange, Node, NodeConfig, SfDatabuffer};
use tokio::net::TcpListener;

#[test]
fn raw_data_00() {
    //taskrun::run(disk::gen::gen_test_data()).unwrap();
    let fut = async {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut con = TcpStream::connect(lis.local_addr().unwrap()).await.unwrap();
        let (client, addr) = lis.accept().await.unwrap();
        let cfg = NodeConfigCached {
            node_config: NodeConfig {
                name: "node_name_dummy".into(),
                cluster: Cluster {
                    backend: "testbackend".into(),
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
        let qu = RawEventsQuery {
            channel: Channel {
                series: None,
                backend: "testbackend".into(),
                name: "scalar-i32-be".into(),
            },
            range: NanoRange {
                beg: SEC,
                end: SEC * 10,
            },
            agg_kind: AggKind::Plain,
            disk_io_tune: DiskIoTune {
                read_sys: netpod::ReadSys::TokioAsyncRead,
                read_buffer_len: 1024 * 4,
                read_queue_len: 1,
            },
            do_decompress: true,
            do_test_main_error: false,
            do_test_stream_error: false,
        };
        let query = EventQueryJsonStringFrame(serde_json::to_string(&qu).unwrap());
        let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(query)));
        let frame = make_frame(&item).unwrap();
        let jh = taskrun::spawn(events_conn_handler(client, addr, cfg));
        con.write_all(&frame).await.unwrap();
        eprintln!("written");
        con.shutdown().await.unwrap();
        eprintln!("shut down");

        let mut frames = InMemoryFrameAsyncReadStream::new(con, 1024 * 128);
        while let Some(frame) = frames.next().await {
            match frame {
                Ok(frame) => match frame {
                    StreamItem::DataItem(k) => {
                        eprintln!("{k:?}");
                        if k.tyid() == items::EVENT_FULL_FRAME_TYPE_ID {
                        } else if k.tyid() == items::ERROR_FRAME_TYPE_ID {
                        } else if k.tyid() == items::LOG_FRAME_TYPE_ID {
                        } else if k.tyid() == items::STATS_FRAME_TYPE_ID {
                        } else {
                            panic!("unexpected frame type id {:x}", k.tyid());
                        }
                        let item: Result<Sitemty<EventFull>, Error> = decode_frame(&k);
                        eprintln!("decoded: {:?}", item);
                    }
                    StreamItem::Log(_) => todo!(),
                    StreamItem::Stats(_) => todo!(),
                },
                Err(e) => {
                    panic!("{e:?}");
                }
            }
        }
        jh.await.unwrap().unwrap();
        Ok(())
    };
    taskrun::run(fut).unwrap();
}
