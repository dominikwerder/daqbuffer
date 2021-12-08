use archapp_xc::ItemSerBox;
use async_channel::Receiver;
use err::Error;
use futures_core::Stream;
use items::Framable;
use netpod::query::RawEventsQuery;
use netpod::{ArchiverAppliance, Channel, ChannelConfigQuery, ChannelConfigResponse, ChannelInfo, NodeConfigCached};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

pub use archapp;

pub fn scan_files(
    pairs: BTreeMap<String, String>,
    node_config: NodeConfigCached,
) -> Pin<Box<dyn Future<Output = Result<Receiver<Result<ItemSerBox, Error>>, Error>> + Send>> {
    Box::pin(archapp::parse::scan_files_inner(
        pairs,
        node_config.node.archiver_appliance.unwrap().data_base_paths,
    ))
}

pub async fn make_event_pipe(
    evq: &RawEventsQuery,
    aa: &ArchiverAppliance,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    archapp::events::make_event_pipe(evq, aa).await
}

pub async fn channel_info(channel: &Channel, node_config: &NodeConfigCached) -> Result<ChannelInfo, Error> {
    archapp::events::channel_info(channel, node_config.node.archiver_appliance.as_ref().unwrap()).await
}

pub async fn channel_config(q: &ChannelConfigQuery, aa: &ArchiverAppliance) -> Result<ChannelConfigResponse, Error> {
    archapp::parse::channel_config(q, aa).await
}
