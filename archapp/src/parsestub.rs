use crate::ItemSer;
use async_channel::Receiver;
use err::Error;
use netpod::NodeConfigCached;
use std::collections::BTreeMap;

type RT1 = Box<dyn ItemSer + Send>;

pub async fn scan_files(
    _pairs: BTreeMap<String, String>,
    _node_config: NodeConfigCached,
) -> Result<Receiver<Result<RT1, Error>>, Error> {
    Err(Error::with_msg("feature not enabled"))
}
