use disk::SfDbChConf;
use err::Error;
use netpod::ChannelTypeConfigGen;
use netpod::NodeConfigCached;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;

async fn find_sf_ch_config_quorum() -> Result<SfChFetchInfo, Error> {
    type _A = SfDbChannel;
    type _B = SfDbChConf;
    // TODO create new endpoint which only returns the most matching config entry
    // for some given channel and time range.
    todo!()
}

pub async fn find_config_basics_quorum(
    channel: &SfDbChannel,
    ncc: &NodeConfigCached,
) -> Result<ChannelTypeConfigGen, Error> {
    if let Some(_cfg) = &ncc.node.sf_databuffer {
        let ret: SfChFetchInfo = find_sf_ch_config_quorum().await?;
        Ok(ChannelTypeConfigGen::SfDatabuffer(ret))
    } else if let Some(_cfg) = &ncc.node_config.cluster.scylla {
        let ret = dbconn::channelconfig::chconf_from_scylla_type_backend(&channel, ncc)
            .await
            .map_err(Error::from)?;
        Ok(ChannelTypeConfigGen::Scylla(ret))
    } else {
        Err(Error::with_msg_no_trace(
            "find_config_basics_quorum  not supported backend",
        ))
    }
}
