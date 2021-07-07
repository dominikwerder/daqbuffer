use crate::parse::PbFileReader;
use err::Error;
use futures_core::Stream;
use items::Framable;
use netpod::query::RawEventsQuery;
use netpod::{ArchiverAppliance, Channel, ChannelInfo, NodeConfigCached, Shape};
use serde_json::Value as JsonValue;
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
    let path2 = a.iter().take(a.len() - 1).fold(path1, |a, &x| a.join(x));
    let mut msgs = vec![];
    msgs.push(format!("a: {:?}", a));
    msgs.push(format!("path2: {}", path2.to_string_lossy()));
    let mut rd = tokio::fs::read_dir(&path2).await?;
    while let Some(de) = rd.next_entry().await? {
        let s = de.file_name().to_string_lossy().into_owned();
        if s.starts_with(a.last().unwrap()) && s.ends_with(".pb") {
            msgs.push(s);
            let f1 = tokio::fs::File::open(de.path()).await?;
            let mut pbr = PbFileReader::new(f1).await;
            pbr.read_header().await?;
            msgs.push(format!("got header {}", pbr.channel_name()));
        }
    }
    let ret = ChannelInfo {
        shape: Shape::Scalar,
        msg: JsonValue::Array(msgs.into_iter().map(JsonValue::String).collect()),
    };
    Ok(ret)
}
