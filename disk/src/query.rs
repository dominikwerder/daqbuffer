use err::Error;
use netpod::Channel;
use std::collections::BTreeMap;

pub fn channel_from_params(params: &BTreeMap<String, String>) -> Result<Channel, Error> {
    let ret = Channel {
        backend: params
            .get("channelBackend")
            .ok_or(Error::with_msg("missing channelBackend"))?
            .into(),
        name: params
            .get("channelName")
            .ok_or(Error::with_msg("missing channelName"))?
            .into(),
    };
    Ok(ret)
}
