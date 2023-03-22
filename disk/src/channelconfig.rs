use err::Error;
use netpod::range::evrange::NanoRange;
use netpod::Channel;
use netpod::ChannelConfig;
use netpod::NodeConfigCached;
use parse::channelconfig::extract_matching_config_entry;
use parse::channelconfig::read_local_config;
use parse::channelconfig::ChannelConfigs;
use parse::channelconfig::MatchingConfigEntry;

pub async fn config(
    range: NanoRange,
    channel: Channel,
    node_config: &NodeConfigCached,
) -> Result<ChannelConfig, Error> {
    let channel_configs = read_local_config(channel.clone(), node_config.node.clone()).await?;
    let entry_res = match extract_matching_config_entry(&range, &channel_configs) {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let entry = match entry_res {
        MatchingConfigEntry::None => {
            return Err(Error::with_public_msg(format!(
                "disk::channelconfig no config entry found for {:?}",
                channel
            )))?
        }
        MatchingConfigEntry::Multiple => {
            return Err(Error::with_public_msg(format!(
                "disk::channelconfig multiple config entries in range found for {:?}",
                channel
            )))?
        }
        MatchingConfigEntry::Entry(entry) => entry,
    };
    let shape = match entry.to_shape() {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    let channel_config = ChannelConfig {
        channel: channel.clone(),
        keyspace: entry.ks as u8,
        time_bin_size: entry.bs.clone(),
        shape,
        scalar_type: entry.scalar_type.clone(),
        byte_order: entry.byte_order.clone(),
        array: entry.is_array,
        compression: entry.is_compressed,
    };
    Ok(channel_config)
}

pub async fn configs(channel: Channel, node_config: &NodeConfigCached) -> Result<ChannelConfigs, Error> {
    read_local_config(channel.clone(), node_config.node.clone()).await
}
