/*
Provide ser/de of value data to a good net exchange format.
*/

use crate::agg::MinMaxAvgScalarBinBatch;
use err::Error;
use futures_core::Stream;
use netpod::Node;
use std::pin::Pin;
use std::task::{Context, Poll};

pub async fn x_processed_stream_from_node(
    node: &Node,
) -> Result<Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>>>>, Error> {
    // TODO can I factor this better?
    // Need a stream of bytes, and a deserializer from stream of bytes to stream of items.
    // Need to pass the parameters to upstream.

    let netin = tokio::net::TcpStream::connect(format!("{}:{}", node.host, node.port_raw)).await?;

    // TODO  TcpStream is not yet a Stream!

    //let s2: Pin<Box<dyn Stream<Item = Result<MinMaxAvgScalarBinBatch, Error>>>> = Box::pin(netin);

    err::todoval()
}

pub struct MinMaxAvgScalarBinBatchStreamFromByteStream {
    //inp: TcpStream,
}

impl Stream for MinMaxAvgScalarBinBatchStreamFromByteStream {
    type Item = Result<MinMaxAvgScalarBinBatch, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        err::todoval()
    }
}

#[allow(dead_code)]
async fn local_unpacked_test() {
    // TODO what kind of query format? What information do I need here?
    // Don't need exact details of channel because I need to parse the databuffer config anyway.

    /*let query = netpod::AggQuerySingleChannel {
        channel_config: ChannelConfig {
            channel: Channel {
                backend: "ks".into(),
                name: "wave1".into(),
            },
            keyspace: 3,
            time_bin_size: DAY,
            shape: Shape::Wave(17),
            scalar_type: ScalarType::F64,
            big_endian: true,
            compression: true,
        },
        timebin: 0,
        tb_file_count: 1,
        buffer_size: 1024 * 8,
    };*/

    let query = err::todoval();
    let node = err::todoval();

    // TODO generate channel configs for my test data.

    // TODO open and parse the channel config.

    // TODO find the matching config entry. (bonus: fuse consecutive compatible entries)

    use crate::agg::IntoDim1F32Stream;
    let _stream = crate::EventBlobsComplete::new(&query, query.channel_config.clone(), node).into_dim_1_f32_stream();
}
