use err::Error;
use netpod::timeunits::MS;
use netpod::{ChannelConfig, Nanos, Node};
use std::path::PathBuf;

pub fn datapath(timebin: u64, config: &netpod::ChannelConfig, node: &Node) -> PathBuf {
    //let pre = "/data/sf-databuffer/daq_swissfel";
    node.data_base_path
        .join(format!("{}_{}", node.ksprefix, config.keyspace))
        .join("byTime")
        .join(config.channel.name.clone())
        .join(format!("{:019}", timebin))
        .join(format!("{:010}", node.split))
        .join(format!(
            "{:019}_00000_Data",
            config.time_bin_size / netpod::timeunits::MS
        ))
}

pub fn channel_timebins_dir_path(channel_config: &ChannelConfig, node: &Node) -> Result<PathBuf, Error> {
    let ret = node
        .data_base_path
        .join(format!("{}_{}", node.ksprefix, channel_config.keyspace))
        .join("byTime")
        .join(&channel_config.channel.name);
    Ok(ret)
}

pub fn data_dir_path(ts: Nanos, channel_config: &ChannelConfig, node: &Node) -> Result<PathBuf, Error> {
    let ret = channel_timebins_dir_path(channel_config, node)?
        .join(format!("{:019}", ts.ns / channel_config.time_bin_size))
        .join(format!("{:010}", node.split));
    Ok(ret)
}

pub fn data_path(ts: Nanos, channel_config: &ChannelConfig, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data", channel_config.time_bin_size / MS, 0);
    let ret = data_dir_path(ts, channel_config, node)?.join(fname);
    Ok(ret)
}

pub fn index_path(ts: Nanos, channel_config: &ChannelConfig, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data_Index", channel_config.time_bin_size / MS, 0);
    let ret = data_dir_path(ts, channel_config, node)?.join(fname);
    Ok(ret)
}
