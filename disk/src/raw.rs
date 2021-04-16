/*
Provide ser/de of value data to a good net exchange format.
*/

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

    let query = todo!();
    let node = todo!();

    // TODO generate channel configs for my test data.

    // TODO open and parse the channel config.

    // TODO find the matching config entry. (bonus: fuse consecutive compatible entries)

    use crate::agg::IntoDim1F32Stream;
    let stream = crate::EventBlobsComplete::new(&query, query.channel_config.clone(), node)
        .into_dim_1_f32_stream();
}
