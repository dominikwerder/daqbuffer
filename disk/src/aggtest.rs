use super::agg::{AggregatableXdim1Bin, IntoBinnedT, IntoBinnedXBins1, IntoDim1F32Stream, ValuesDim1};
use super::merge::MergeDim1F32Stream;
use crate::agg::make_test_node;
use futures_util::StreamExt;
use netpod::timeunits::*;
use netpod::{BinSpecDimT, Channel, ChannelConfig, NanoRange, ScalarType, Shape};
use std::future::ready;
use std::sync::Arc;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

#[test]
fn agg_x_dim_0() {
    taskrun::run(async {
        agg_x_dim_0_inner().await;
        Ok(())
    })
    .unwrap();
}

async fn agg_x_dim_0_inner() {
    let node = make_test_node(0);
    let node = Arc::new(node);
    let query = netpod::AggQuerySingleChannel {
        channel_config: ChannelConfig {
            channel: Channel {
                backend: "sf-databuffer".into(),
                name: "S10BC01-DBAM070:EOM1_T1".into(),
            },
            keyspace: 2,
            time_bin_size: DAY,
            array: false,
            shape: Shape::Scalar,
            scalar_type: ScalarType::F64,
            big_endian: true,
            compression: true,
        },
        timebin: 18723,
        tb_file_count: 1,
        buffer_size: 1024 * 4,
    };
    let bin_count = 20;
    let ts1 = query.timebin as u64 * query.channel_config.time_bin_size;
    let ts2 = ts1 + HOUR * 24;
    let range = NanoRange { beg: ts1, end: ts2 };
    let fut1 = super::eventblobs::EventBlobsComplete::new(&query, query.channel_config.clone(), range, node)
        .into_dim_1_f32_stream()
        //.take(1000)
        .map(|q| {
            if false {
                if let Ok(ref k) = q {
                    trace!("vals: {:?}", k);
                }
            }
            q
        })
        .into_binned_x_bins_1()
        .map(|k| {
            if false {
                trace!("after X binning  {:?}", k.as_ref().unwrap());
            }
            k
        })
        .into_binned_t(BinSpecDimT::over_range(bin_count, ts1, ts2))
        .map(|k| {
            if false {
                trace!("after T binning  {:?}", k.as_ref().unwrap());
            }
            k
        })
        .for_each(|_k| ready(()));
    fut1.await;
}

#[test]
fn agg_x_dim_1() {
    taskrun::run(async {
        agg_x_dim_1_inner().await;
        Ok(())
    })
    .unwrap();
}

async fn agg_x_dim_1_inner() {
    // sf-databuffer
    // /data/sf-databuffer/daq_swissfel/daq_swissfel_3/byTime/S10BC01-DBAM070\:BAM_CH1_NORM/*
    // S10BC01-DBAM070:BAM_CH1_NORM
    let node = make_test_node(0);
    let node = Arc::new(node);
    let query = netpod::AggQuerySingleChannel {
        channel_config: ChannelConfig {
            channel: Channel {
                backend: "ks".into(),
                name: "wave1".into(),
            },
            keyspace: 3,
            time_bin_size: DAY,
            array: true,
            shape: Shape::Wave(1024),
            scalar_type: ScalarType::F64,
            big_endian: true,
            compression: true,
        },
        timebin: 0,
        tb_file_count: 1,
        buffer_size: 17,
    };
    let bin_count = 10;
    let ts1 = query.timebin as u64 * query.channel_config.time_bin_size;
    let ts2 = ts1 + HOUR * 24;
    let range = NanoRange { beg: ts1, end: ts2 };
    let fut1 = super::eventblobs::EventBlobsComplete::new(&query, query.channel_config.clone(), range, node)
        .into_dim_1_f32_stream()
        //.take(1000)
        .map(|q| {
            if false {
                if let Ok(ref k) = q {
                    info!("vals: {:?}", k);
                }
            }
            q
        })
        .into_binned_x_bins_1()
        .map(|k| {
            //info!("after X binning  {:?}", k.as_ref().unwrap());
            k
        })
        .into_binned_t(BinSpecDimT::over_range(bin_count, ts1, ts2))
        .map(|k| {
            info!("after T binning  {:?}", k.as_ref().unwrap());
            k
        })
        .for_each(|_k| ready(()));
    fut1.await;
}

#[test]
fn merge_0() {
    taskrun::run(async {
        merge_0_inner().await;
        Ok(())
    })
    .unwrap();
}

async fn merge_0_inner() {
    let query = netpod::AggQuerySingleChannel {
        channel_config: ChannelConfig {
            channel: Channel {
                backend: "ks".into(),
                name: "wave1".into(),
            },
            keyspace: 3,
            time_bin_size: DAY,
            array: true,
            shape: Shape::Wave(17),
            scalar_type: ScalarType::F64,
            big_endian: true,
            compression: true,
        },
        timebin: 0,
        tb_file_count: 1,
        buffer_size: 1024 * 8,
    };
    let range: NanoRange = err::todoval();
    let streams = (0..13)
        .into_iter()
        .map(|k| make_test_node(k))
        .map(|node| {
            let node = Arc::new(node);
            super::eventblobs::EventBlobsComplete::new(&query, query.channel_config.clone(), range.clone(), node)
                .into_dim_1_f32_stream()
        })
        .collect();
    MergeDim1F32Stream::new(streams)
        .map(|_k| {
            //info!("NEXT MERGED ITEM  ts {:?}", k.as_ref().unwrap().tss);
        })
        .fold(0, |_k, _q| ready(0))
        .await;
}

pub fn tmp_some_older_things() {
    let vals = ValuesDim1 {
        tss: vec![0, 1, 2, 3],
        values: vec![vec![0., 0., 0.], vec![1., 1., 1.], vec![2., 2., 2.], vec![3., 3., 3.]],
    };
    // I want to distinguish already in the outer part between dim-0 and dim-1 and generate
    // separate code for these cases...
    // That means that also the reading chain itself needs to be typed on that.
    // Need to supply some event-payload converter type which has that type as Output type.
    let _vals2 = vals.into_agg();
    // Now the T-binning:

    /*
    T-aggregator must be able to produce empty-values of correct type even if we never get
    a single value of input data.
    Therefore, it needs the bin range definition.
    How do I want to drive the system?
    If I write the T-binner as a Stream, then I also need to pass it the input!
    Meaning, I need to pass the Stream which produces the actual numbers from disk.

    readchannel()  -> Stream of timestamped byte blobs
    .to_f32()  -> Stream ?    indirection to branch on the underlying shape
    .agg_x_bins_1()  -> Stream ?    can I keep it at the single indirection on the top level?
    */
}
