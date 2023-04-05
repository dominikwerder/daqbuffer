use dbconn::query::sf_databuffer_fetch_channel_by_series;
use err::Error;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::ChConf;
use netpod::Channel;
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
                series: Some(1),
                name: channel.name().into(),
                scalar_type: ScalarType::I32,
                shape: Shape::Scalar,
            };
            Ok(ret)
        } else if channel.name() == "wave-f64-be-n21" {
            let ret = ChConf {
                backend,
                series: Some(2),
                name: channel.name().into(),
                scalar_type: ScalarType::F64,
                shape: Shape::Wave(21),
            };
            Ok(ret)
        } else if channel.name() == "const-regular-scalar-i32-be" {
            let ret = ChConf {
                backend,
                series: Some(3),
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
                series: Some(1),
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
        info!("channel_config  BEFORE  {channel:?}");
        info!("try to get ChConf for sf-databuffer type backend");
        // TODO in the future we should not need this:
        let mut channel = sf_databuffer_fetch_channel_by_series(channel, ncc).await?;
        if channel.series.is_none() {
            let pgclient = dbconn::create_connection(&ncc.node_config.cluster.database).await?;
            let pgclient = std::sync::Arc::new(pgclient);
            let series = dbconn::find_series_sf_databuffer(&channel, pgclient).await?;
            channel.series = Some(series);
        }
        let channel = channel;
        info!("channel_config  AFTER  {channel:?}");
        let c1 = disk::channelconfig::config(range, channel.clone(), ncc).await?;
        info!("channel_config  THEN  {c1:?}");
        let ret = ChConf {
            backend: c1.channel.backend,
            series: channel.series,
            name: c1.channel.name,
            scalar_type: c1.scalar_type,
            shape: c1.shape,
        };
        Ok(ret)
    } else {
        err::todoval()
    }
}