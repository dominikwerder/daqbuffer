use crate::conn::events_conn_handler;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::ERROR_FRAME_TYPE_ID;
use items_0::streamitem::ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID;
use items_0::streamitem::LOG_FRAME_TYPE_ID;
use items_0::streamitem::STATS_FRAME_TYPE_ID;
use items_2::channelevents::ChannelEvents;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::frame::decode_frame;
use items_2::frame::make_frame;
use netpod::query::PlainEventsQuery;
use netpod::timeunits::SEC;
use netpod::AggKind;
use netpod::Channel;
use netpod::Cluster;
use netpod::Database;
use netpod::FileIoBufferSize;
use netpod::NanoRange;
use netpod::Node;
use netpod::NodeConfig;
use netpod::NodeConfigCached;
use netpod::SfDatabuffer;
use std::time::Duration;
use streams::frames::inmem::InMemoryFrameAsyncReadStream;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[test]
fn raw_data_00() {
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
        let channel = Channel {
            series: None,
            backend: "test-adhoc-dyn".into(),
            name: "scalar-i32".into(),
        };
        let range = NanoRange {
            beg: SEC,
            end: SEC * 10,
        };
        let qu = PlainEventsQuery::new(channel, range);
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
                        if k.tyid() == ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID {
                        } else if k.tyid() == ERROR_FRAME_TYPE_ID {
                        } else if k.tyid() == LOG_FRAME_TYPE_ID {
                        } else if k.tyid() == STATS_FRAME_TYPE_ID {
                        } else {
                            panic!("unexpected frame type id {:x}", k.tyid());
                        }
                        let item: Sitemty<ChannelEvents> = decode_frame(&k).unwrap();
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
