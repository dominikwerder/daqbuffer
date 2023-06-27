use err::Error;
use httpclient::url::Url;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::DAY;
use netpod::AppendToUrl;
use netpod::ByteOrder;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::ChannelTypeConfigGen;
use netpod::DtNano;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::APP_JSON;

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
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    if channel.backend() == TEST_BACKEND {
        Ok(Some(channel_config_test_backend(channel)?))
    } else if ncc.node_config.cluster.scylla.is_some() {
        debug!("try to get ChConf for scylla type backend");
        let ret = dbconn::channelconfig::chconf_from_scylla_type_backend(&channel, ncc)
            .await
            .map_err(Error::from)?;
        Ok(Some(ChannelTypeConfigGen::Scylla(ret)))
    } else if ncc.node.sf_databuffer.is_some() {
        debug!("channel_config  channel {channel:?}");
        let k = disk::channelconfig::channel_config_best_match(range, channel.clone(), ncc)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        match k {
            Some(config) => {
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
                Ok(Some(ret))
            }
            None => Ok(None),
        }
    } else {
        return Err(
            Error::with_msg_no_trace(format!("no channel config for backend {}", channel.backend()))
                .add_public_msg(format!("no channel config for backend {}", channel.backend())),
        );
    }
}

pub async fn channel_configs(channel: SfDbChannel, ncc: &NodeConfigCached) -> Result<Vec<ChannelTypeConfigGen>, Error> {
    if channel.backend() == TEST_BACKEND {
        let x = vec![channel_config_test_backend(channel)?];
        Ok(x)
    } else if ncc.node_config.cluster.scylla.is_some() {
        debug!("try to get ChConf for scylla type backend");
        let ret = dbconn::channelconfig::chconf_from_scylla_type_backend(&channel, ncc)
            .await
            .map_err(Error::from)?;
        Ok(vec![ChannelTypeConfigGen::Scylla(ret)])
    } else if ncc.node.sf_databuffer.is_some() {
        debug!("channel_config  channel {channel:?}");
        let configs = disk::channelconfig::channel_configs(channel.clone(), ncc)
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        let a = configs;
        let mut configs = Vec::new();
        for config in a.entries {
            let ret = SfChFetchInfo::new(
                channel.backend(),
                channel.name(),
                config.ks as _,
                config.bs,
                config.byte_order,
                config.scalar_type,
                Shape::from_sf_databuffer_raw(&config.shape)?,
            );
            let ret = ChannelTypeConfigGen::SfDatabuffer(ret);
            configs.push(ret);
        }
        Ok(configs)
    } else {
        return Err(
            Error::with_msg_no_trace(format!("no channel config for backend {}", channel.backend()))
                .add_public_msg(format!("no channel config for backend {}", channel.backend())),
        );
    }
}

pub async fn http_get_channel_config(
    qu: ChannelConfigQuery,
    baseurl: Url,
) -> Result<Option<ChannelConfigResponse>, Error> {
    let url = baseurl;
    let mut url = url.join("channel/config").unwrap();
    qu.append_to_url(&mut url);
    let res = httpclient::http_get(url, APP_JSON).await?;
    use httpclient::http::StatusCode;
    if res.head.status == StatusCode::NOT_FOUND {
        Ok(None)
    } else if res.head.status == StatusCode::OK {
        let ret: ChannelConfigResponse = serde_json::from_slice(&res.body)?;
        Ok(Some(ret))
    } else {
        let b = &res.body;
        let s = String::from_utf8_lossy(&b[0..b.len().min(256)]);
        Err(Error::with_msg_no_trace(format!(
            "http_get_channel_config  {}  {}",
            res.head.status, s
        )))
    }
}
