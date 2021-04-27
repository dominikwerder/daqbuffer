use err::Error;
use netpod::log::*;
use netpod::{ChannelConfig, Nanos, Node};
use tokio::fs::OpenOptions;
use tokio::io::ErrorKind;

pub async fn find_start_pos_for_config(
    ts: Nanos,
    channel_config: &ChannelConfig,
    node: &Node,
) -> Result<Option<u64>, Error> {
    let index_path = super::paths::index_path(ts, channel_config, node)?;
    let ret = match OpenOptions::new().open(&index_path).await {
        Ok(_file) => {
            info!("opened index file");
            error!("???????????????    TODO   search index for start");
            err::todoval::<u32>();
            None
        }
        Err(e) => match e.kind() {
            ErrorKind::NotFound => None,
            _ => Err(e)?,
        },
    };
    Ok(ret)
}
