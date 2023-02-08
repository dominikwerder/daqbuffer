use err::Error;
use netpod::log::*;
use netpod::ChConf;
use netpod::Channel;
use netpod::NanoRange;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::Shape;

pub async fn channel_config(range: NanoRange, channel: Channel, ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    if channel.backend() == "test-disk-databuffer" {
        let backend = channel.backend().into();
        // TODO the series-ids here are just random. Need to integrate with better test setup.
        let ret = if channel.name() == "scalar-i32-be" {
            let ret = ChConf {
                backend,
                series: 1,
                name: channel.name().into(),
                scalar_type: ScalarType::I32,
                shape: Shape::Scalar,
            };
            Ok(ret)
        } else if channel.name() == "wave-f64-be-n21" {
            let ret = ChConf {
                backend,
                series: 2,
                name: channel.name().into(),
                scalar_type: ScalarType::F64,
                shape: Shape::Wave(21),
            };
            Ok(ret)
        } else if channel.name() == "const-regular-scalar-i32-be" {
            let ret = ChConf {
                backend,
                series: 3,
                name: channel.name().into(),
                scalar_type: ScalarType::I32,
                shape: Shape::Scalar,
            };
            Ok(ret)
        } else {
            error!("no test information");
            Err(Error::with_msg_no_trace(format!("no test information"))
                .add_public_msg("No channel config for test channel {:?}"))
        };
        ret
    } else if channel.backend() == "test-inmem" {
        let backend = channel.backend().into();
        let ret = if channel.name() == "inmem-d0-i32" {
            let ret = ChConf {
                backend,
                series: 1,
                name: channel.name().into(),
                scalar_type: ScalarType::I32,
                shape: Shape::Scalar,
            };
            Ok(ret)
        } else {
            error!("no test information");
            Err(Error::with_msg_no_trace(format!("no test information"))
                .add_public_msg("No channel config for test channel {:?}"))
        };
        ret
    } else if ncc.node_config.cluster.scylla.is_some() {
        info!("try to get ChConf for scylla type backend");
        let ret = dbconn::channelconfig::chconf_from_scylla_type_backend(&channel, ncc)
            .await
            .map_err(Error::from)?;
        Ok(ret)
    } else if ncc.node.sf_databuffer.is_some() {
        info!("try to get ChConf for sf-databuffer type backend");
        let c1 = disk::channelconfig::config(range, channel, ncc).await?;
        let ret = ChConf {
            backend: c1.channel.backend,
            series: 0,
            name: c1.channel.name,
            scalar_type: c1.scalar_type,
            shape: c1.shape,
        };
        Ok(ret)
    } else {
        err::todoval()
    }
}
