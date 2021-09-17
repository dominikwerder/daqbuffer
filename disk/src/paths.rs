use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits::MS;
use netpod::{ChannelConfig, Nanos, Node};
use std::path::PathBuf;

// TODO remove/replace this
pub fn datapath(timebin: u64, config: &netpod::ChannelConfig, split: u32, node: &Node) -> PathBuf {
    node.data_base_path
        .join(format!("{}_{}", node.ksprefix, config.keyspace))
        .join("byTime")
        .join(config.channel.name.clone())
        .join(format!("{:019}", timebin))
        .join(format!("{:010}", split))
        .join(format!("{:019}_00000_Data", config.time_bin_size.ns / MS))
}

/**
Return potential datafile paths for the given timebin.

It says "potential datafile paths" because we don't open the file here yet and of course,
files may vanish until then. Also, the timebin may actually not exist.
*/
pub async fn datapaths_for_timebin(
    timebin: u64,
    config: &netpod::ChannelConfig,
    node: &Node,
) -> Result<Vec<PathBuf>, Error> {
    let timebin_path = node
        .data_base_path
        .join(format!("{}_{}", node.ksprefix, config.keyspace))
        .join("byTime")
        .join(config.channel.name.clone())
        .join(format!("{:019}", timebin));
    let rd = tokio::fs::read_dir(timebin_path).await?;
    let mut rd = tokio_stream::wrappers::ReadDirStream::new(rd);
    let mut splits = vec![];
    while let Some(e) = rd.next().await {
        let e = e?;
        let dn = e
            .file_name()
            .into_string()
            .map_err(|s| Error::with_msg(format!("Bad OS path {:?}", s)))?;
        if dn.len() != 10 {
            return Err(Error::with_msg(format!("bad split dirname {:?}", e)));
        }
        let vv = dn.chars().fold(0, |a, x| if x.is_digit(10) { a + 1 } else { a });
        if vv == 10 {
            splits.push(dn.parse::<u64>()?);
        }
    }
    let mut ret = vec![];
    for split in splits {
        let path = node
            .data_base_path
            .join(format!("{}_{}", node.ksprefix, config.keyspace))
            .join("byTime")
            .join(config.channel.name.clone())
            .join(format!("{:019}", timebin))
            .join(format!("{:010}", split))
            .join(format!("{:019}_00000_Data", config.time_bin_size.ns / MS));
        ret.push(path);
    }
    if false {
        info!("datapaths_for_timebin returns: {:?}", ret)
    }
    Ok(ret)
}

pub fn channel_timebins_dir_path(channel_config: &ChannelConfig, node: &Node) -> Result<PathBuf, Error> {
    let ret = node
        .data_base_path
        .join(format!("{}_{}", node.ksprefix, channel_config.keyspace))
        .join("byTime")
        .join(&channel_config.channel.name);
    Ok(ret)
}

pub fn data_dir_path(ts: Nanos, channel_config: &ChannelConfig, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let ret = channel_timebins_dir_path(channel_config, node)?
        .join(format!("{:019}", ts.ns / channel_config.time_bin_size.ns))
        .join(format!("{:010}", split));
    Ok(ret)
}

pub fn data_path(ts: Nanos, channel_config: &ChannelConfig, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data", channel_config.time_bin_size.ns / MS, 0);
    let ret = data_dir_path(ts, channel_config, split, node)?.join(fname);
    Ok(ret)
}

pub fn index_path(ts: Nanos, channel_config: &ChannelConfig, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data_Index", channel_config.time_bin_size.ns / MS, 0);
    let ret = data_dir_path(ts, channel_config, split, node)?.join(fname);
    Ok(ret)
}

pub fn data_dir_path_tb(ks: u32, channel_name: &str, tb: u32, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let ret = node
        .data_base_path
        .join(format!("{}_{}", node.ksprefix, ks))
        .join("byTime")
        .join(channel_name)
        .join(format!("{:019}", tb))
        .join(format!("{:010}", split));
    Ok(ret)
}

pub fn data_path_tb(ks: u32, channel_name: &str, tb: u32, tbs: u32, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data", tbs, 0);
    let ret = data_dir_path_tb(ks, channel_name, tb, split, node)?.join(fname);
    Ok(ret)
}
