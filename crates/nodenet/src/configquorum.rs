use crate::channelconfig::http_get_channel_config;
use err::Error;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::ChConf;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::ChannelTypeConfigGen;
use netpod::DtNano;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;
use std::collections::BTreeMap;
use std::time::Duration;
use taskrun::tokio;

fn decide_sf_ch_config_quorum(inp: Vec<ChannelConfigResponse>) -> Result<Option<ChannelTypeConfigGen>, Error> {
    let mut histo = BTreeMap::new();
    for item in inp {
        let item = match item {
            ChannelConfigResponse::SfDatabuffer(k) => ChannelTypeConfigGen::SfDatabuffer(SfChFetchInfo::new(
                k.backend,
                k.name,
                k.keyspace,
                DtNano::from_ms(k.timebinsize),
                k.byte_order,
                k.scalar_type,
                k.shape,
            )),
            ChannelConfigResponse::Daqbuf(k) => {
                ChannelTypeConfigGen::Scylla(ChConf::new(k.backend, k.series, k.scalar_type, k.shape, k.name))
            }
        };
        if histo.contains_key(&item) {
            *histo.get_mut(&item).unwrap() += 1;
        } else {
            histo.insert(item, 0u32);
        }
    }
    let mut v: Vec<_> = histo.into_iter().collect();
    v.sort_unstable_by_key(|x| x.1);
    match v.pop() {
        Some((x, _)) => Ok(Some(x)),
        None => Ok(None),
    }
}

async fn find_sf_ch_config_quorum(
    channel: SfDbChannel,
    range: SeriesRange,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<Option<SfChFetchInfo>, Error> {
    let range = match range {
        SeriesRange::TimeRange(x) => x,
        SeriesRange::PulseRange(_) => return Err(Error::with_msg_no_trace("expect TimeRange")),
    };
    let mut all = Vec::new();
    for node in &ncc.node_config.cluster.nodes {
        // TODO add a baseurl function to struct Node
        let qu = ChannelConfigQuery {
            channel: channel.clone(),
            range: range.clone(),
            // TODO
            expand: false,
        };
        let res = tokio::time::timeout(
            Duration::from_millis(4000),
            http_get_channel_config(qu, node.baseurl(), ctx),
        )
        .await
        .map_err(|_| Error::with_msg_no_trace("timeout"))??;
        all.push(res);
    }
    let all: Vec<_> = all.into_iter().filter_map(|x| x).collect();
    let qu = decide_sf_ch_config_quorum(all)?;
    match qu {
        Some(item) => match item {
            ChannelTypeConfigGen::Scylla(_) => Err(Error::with_msg_no_trace(
                "find_sf_ch_config_quorum  not a sf-databuffer config",
            )),
            ChannelTypeConfigGen::SfDatabuffer(item) => Ok(Some(item)),
        },
        None => Ok(None),
    }
}

pub async fn find_config_basics_quorum(
    channel: SfDbChannel,
    range: SeriesRange,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    if let Some(_cfg) = &ncc.node.sf_databuffer {
        let channel = if channel.name().is_empty() {
            if let Some(_) = channel.series() {
                let pgclient = dbconn::create_connection(&ncc.node_config.cluster.database).await?;
                let pgclient = std::sync::Arc::new(pgclient);
                dbconn::find_sf_channel_by_series(channel, pgclient)
                    .await
                    .map_err(|e| Error::with_msg_no_trace(e.to_string()))?
            } else {
                channel
            }
        } else {
            channel
        };
        match find_sf_ch_config_quorum(channel, range, ctx, ncc).await? {
            Some(x) => Ok(Some(ChannelTypeConfigGen::SfDatabuffer(x))),
            None => Ok(None),
        }
    } else if let Some(_cfg) = &ncc.node_config.cluster.scylla {
        // TODO let called function allow to return None instead of error-not-found
        let ret = dbconn::channelconfig::chconf_from_scylla_type_backend(&channel, ncc)
            .await
            .map_err(Error::from)?;
        Ok(Some(ChannelTypeConfigGen::Scylla(ret)))
    } else {
        Err(Error::with_msg_no_trace(
            "find_config_basics_quorum  not supported backend",
        ))
    }
}
