use err::Error;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::DAY;
use netpod::ByteOrder;
use netpod::ChannelTypeConfigGen;
use netpod::DtNano;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;
use netpod::Shape;

const TEST_BACKEND: &str = "testbackend-00";

fn channel_config_test_backend(channel: SfDbChannel) -> Result<ChannelTypeConfigGen, Error> {
    let backend = channel.backend();
    let ret = if channel.name() == "scalar-i32-be" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "wave-f64-be-n21" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            3,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::F64,
            Shape::Wave(21),
        );
        ret
    } else if channel.name() == "const-regular-scalar-i32-be" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "test-gen-i32-dim0-v00" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "test-gen-i32-dim0-v01" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "test-gen-f64-dim1-v00" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            3,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::F64,
            Shape::Wave(21),
        );
        ret
    } else {
        error!("no test information");
        return Err(Error::with_msg_no_trace(format!("no test information"))
            .add_public_msg("No channel config for test channel {:?}"));
    };
    Ok(ChannelTypeConfigGen::SfDatabuffer(ret))
}

pub async fn channel_config(
    range: NanoRange,
    channel: SfDbChannel,
    ncc: &NodeConfigCached,
) -> Result<ChannelTypeConfigGen, Error> {
    if channel.backend() == TEST_BACKEND {
        channel_config_test_backend(channel)
    } else if ncc.node_config.cluster.scylla.is_some() {
        debug!("try to get ChConf for scylla type backend");
        let ret = dbconn::channelconfig::chconf_from_scylla_type_backend(&channel, ncc)
            .await
            .map_err(Error::from)?;
        Ok(ChannelTypeConfigGen::Scylla(ret))
    } else if ncc.node.sf_databuffer.is_some() {
        debug!("channel_config  channel {channel:?}");
        let config = disk::channelconfig::channel_config_best_match(range, channel.clone(), ncc)
            .await?
            .ok_or_else(|| Error::with_msg_no_trace("config entry not found"))?;
        debug!("channel_config  config  {config:?}");
        let ret = SfChFetchInfo::new(
            config.channel.backend(),
            config.channel.name(),
            config.keyspace,
            config.time_bin_size,
            config.byte_order,
            config.scalar_type,
            config.shape,
        );
        let ret = ChannelTypeConfigGen::SfDatabuffer(ret);
        Ok(ret)
    } else {
        return Err(
            Error::with_msg_no_trace(format!("no channel config for backend {}", channel.backend()))
                .add_public_msg(format!("no channel config for backend {}", channel.backend())),
        );
    }
}
