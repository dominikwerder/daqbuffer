use err::Error;
use futures_core::Stream;
use items::Framable;
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::{ArchiverAppliance, Channel, ChannelInfo, NodeConfigCached, Shape};
use std::pin::Pin;

pub async fn make_event_pipe(
    _evq: &RawEventsQuery,
    _aa: &ArchiverAppliance,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    err::todoval()
}

pub async fn channel_info(channel: &Channel, node_config: &NodeConfigCached) -> Result<ChannelInfo, Error> {
    // SARUN11/CVME/DBLM546/IOC_CPU_LOAD
    // SARUN11-CVME-DBLM546:IOC_CPU_LOAD
    let a: Vec<_> = channel.name.split("-").map(|s| s.split(":")).flatten().collect();
    let path1 = node_config
        .node
        .archiver_appliance
        .as_ref()
        .unwrap()
        .data_base_path
        .clone();
    let path2 = a.iter().fold(path1, |a, &x| a.join(x));
    info!("path2: {}", path2.to_str().unwrap());
    let ret = ChannelInfo {
        shape: Shape::Scalar,
        msg: format!("{:?}  path2: {:?}", a, path2),
    };
    Ok(ret)
}
