use crate::channelconfig::http_get_channel_config;
use disk::SfDbChConf;
use err::Error;
use httpclient::url::Url;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::ChannelConfigQuery;
use netpod::ChannelTypeConfigGen;
use netpod::NodeConfigCached;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;

async fn find_sf_ch_config_quorum(
    channel: SfDbChannel,
    range: SeriesRange,
    ncc: &NodeConfigCached,
) -> Result<SfChFetchInfo, Error> {
    let range = match range {
        SeriesRange::TimeRange(x) => x,
        SeriesRange::PulseRange(_) => return Err(Error::with_msg_no_trace("expect TimeRange")),
    };
    type _A = SfDbChannel;
    type _B = SfDbChConf;
    let mut ress = Vec::new();
    for node in &ncc.node_config.cluster.nodes {
        // TODO add a baseurl function to struct Node
        let baseurl: Url = format!("http://{}:{}/api/4/", node.host, node.port).parse()?;
        let qu = ChannelConfigQuery {
            channel: channel.clone(),
            range: range.clone(),
            // TODO
            expand: false,
        };
        let res = http_get_channel_config(qu, baseurl.clone()).await?;
        info!("GOT: {res:?}");
        ress.push(res);
    }
    // TODO find most likely values.

    // TODO create new endpoint which only returns the most matching config entry
    // for some given channel and time range.
    todo!()
}

pub async fn find_config_basics_quorum(
    channel: SfDbChannel,
    range: SeriesRange,
    ncc: &NodeConfigCached,
) -> Result<ChannelTypeConfigGen, Error> {
    if let Some(_cfg) = &ncc.node.sf_databuffer {
        let ret: SfChFetchInfo = find_sf_ch_config_quorum(channel, range, ncc).await?;
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
