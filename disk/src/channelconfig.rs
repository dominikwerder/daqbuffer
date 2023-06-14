use crate::SfDbChConf;
use err::Error;
use netpod::range::evrange::NanoRange;
use netpod::NodeConfigCached;
use netpod::SfDbChannel;
use parse::channelconfig::extract_matching_config_entry;
use parse::channelconfig::read_local_config;
use parse::channelconfig::ChannelConfigs;
use parse::channelconfig::ConfigEntry;

pub async fn config_entry_best_match(
    range: &NanoRange,
    channel: SfDbChannel,
    node_config: &NodeConfigCached,
) -> Result<Option<ConfigEntry>, Error> {
    let channel_config = read_local_config(channel.clone(), node_config.clone()).await?;
    let entry_res = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    match entry_res.best() {
        None => Ok(None),
        Some(x) => Ok(Some(x.clone())),
    }
}

pub async fn configs(channel: SfDbChannel, node_config: &NodeConfigCached) -> Result<ChannelConfigs, Error> {
    read_local_config(channel.clone(), node_config.clone()).await
}

pub async fn channel_config_best_match(
    range: NanoRange,
    channel: SfDbChannel,
    node_config: &NodeConfigCached,
) -> Result<Option<SfDbChConf>, Error> {
    let best = config_entry_best_match(&range, channel.clone(), node_config).await?;
    let channel_configs = read_local_config(channel.clone(), node_config.clone()).await?;
    let entry_res = match extract_matching_config_entry(&range, &channel_configs) {
        Ok(k) => k,
        Err(e) => return Err(e),
    };
    match entry_res.best() {
        None => Ok(None),
        Some(entry) => {
            let shape = match entry.to_shape() {
                Ok(k) => k,
                Err(e) => return Err(e)?,
            };
            let channel_config = SfDbChConf {
                channel: channel.clone(),
                keyspace: entry.ks as u8,
                time_bin_size: entry.bs.clone(),
                shape,
                scalar_type: entry.scalar_type.clone(),
                byte_order: entry.byte_order.clone(),
                array: entry.is_array,
                compression: entry.is_compressed,
            };
            Ok(Some(channel_config))
        }
    }
}
