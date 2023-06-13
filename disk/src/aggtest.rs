use crate::eventblobs::EventChunkerMultifile;
use crate::eventchunker::EventChunkerConf;
use crate::AggQuerySingleChannel;
use crate::SfDbChConf;
use err::Error;
use netpod::range::evrange::NanoRange;
use netpod::test_data_base_path_databuffer;
use netpod::timeunits::*;
use netpod::ByteOrder;
use netpod::ByteSize;
use netpod::DiskIoTune;
use netpod::Node;
use netpod::ScalarType;
use netpod::SfDatabuffer;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::TsNano;

pub fn make_test_node(id: u32) -> Node {
    Node {
        host: "localhost".into(),
        listen: "0.0.0.0".into(),
        port: 8800 + id as u16,
        port_raw: 8800 + id as u16 + 100,
        // TODO use a common function to supply the tmp path.
        cache_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
        sf_databuffer: Some(SfDatabuffer {
            data_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
            ksprefix: "ks".into(),
            splits: None,
        }),
        archiver_appliance: None,
        channel_archiver: None,
        prometheus_api_bind: None,
    }
}

#[test]
fn agg_x_dim_0() {
    taskrun::run(async {
        agg_x_dim_0_inner().await;
        Ok::<_, Error>(())
    })
    .unwrap();
}

async fn agg_x_dim_0_inner() {
    let node = make_test_node(0);
    let query = AggQuerySingleChannel {
        channel_config: SfDbChConf {
            channel: SfDbChannel {
                backend: "sf-databuffer".into(),
                name: "S10BC01-DBAM070:EOM1_T1".into(),
                series: None,
            },
            keyspace: 2,
            time_bin_size: TsNano(DAY),
            array: false,
            shape: Shape::Scalar,
            scalar_type: ScalarType::F64,
            byte_order: ByteOrder::Big,
            compression: true,
        },
        timebin: 18723,
        tb_file_count: 1,
        buffer_size: 1024 * 4,
    };
    let _bin_count = 20;
    let ts1 = query.timebin as u64 * query.channel_config.time_bin_size.0;
    let ts2 = ts1 + HOUR * 24;
    let range = NanoRange { beg: ts1, end: ts2 };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    // TODO let upstream already provide DiskIoTune:
    let mut disk_io_tune = DiskIoTune::default_for_testing();
    disk_io_tune.read_buffer_len = query.buffer_size as usize;
    let fut1 = EventChunkerMultifile::new(
        range.clone(),
        query.channel_config.clone(),
        node.clone(),
        0,
        disk_io_tune,
        event_chunker_conf,
        false,
        true,
        // TODO
        32,
    );
    let _ = fut1;
    // TODO add the binning and expectation and await the result.
}

#[test]
fn agg_x_dim_1() {
    taskrun::run(async {
        agg_x_dim_1_inner().await;
        Ok::<_, Error>(())
    })
    .unwrap();
}

async fn agg_x_dim_1_inner() {
    // sf-databuffer
    // /data/sf-databuffer/daq_swissfel/daq_swissfel_3/byTime/S10BC01-DBAM070\:BAM_CH1_NORM/*
    // S10BC01-DBAM070:BAM_CH1_NORM
    let node = make_test_node(0);
    let query = AggQuerySingleChannel {
        channel_config: SfDbChConf {
            channel: SfDbChannel {
                backend: "ks".into(),
                name: "wave1".into(),
                series: None,
            },
            keyspace: 3,
            time_bin_size: TsNano(DAY),
            array: true,
            shape: Shape::Wave(1024),
            scalar_type: ScalarType::F64,
            byte_order: ByteOrder::Big,
            compression: true,
        },
        timebin: 0,
        tb_file_count: 1,
        buffer_size: 17,
    };
    let _bin_count = 10;
    let ts1 = query.timebin as u64 * query.channel_config.time_bin_size.0;
    let ts2 = ts1 + HOUR * 24;
    let range = NanoRange { beg: ts1, end: ts2 };
    let event_chunker_conf = EventChunkerConf::new(ByteSize::kb(1024));
    // TODO let upstream already provide DiskIoTune:
    let mut disk_io_tune = DiskIoTune::default_for_testing();
    disk_io_tune.read_buffer_len = query.buffer_size as usize;
    let fut1 = super::eventblobs::EventChunkerMultifile::new(
        range.clone(),
        query.channel_config.clone(),
        node.clone(),
        0,
        disk_io_tune,
        event_chunker_conf,
        false,
        true,
        // TODO
        32,
    );
    let _ = fut1;
    // TODO add the binning and expectation and await the result.
}
