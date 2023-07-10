use crate::create_connection;
use crate::ErrConv;
use err::Error;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::SfDbChannel;

// For sf-databuffer backend, given a Channel, try to complete the information if only id is given.
#[allow(unused)]
async fn sf_databuffer_fetch_channel_by_series(
    channel: SfDbChannel,
    ncc: &NodeConfigCached,
) -> Result<SfDbChannel, Error> {
    let me = "sf_databuffer_fetch_channel_by_series";
    info!("{me}");
    // TODO should not be needed at some point.
    if channel.backend().is_empty() || channel.name().is_empty() {
        if let Some(series) = channel.series() {
            if series < 1 {
                error!("{me}  bad input: {channel:?}");
                Err(Error::with_msg_no_trace(format!("{me}  bad input: {channel:?}")))
            } else {
                info!("{me} do the lookup");
                let series = channel
                    .series()
                    .ok_or_else(|| Error::with_msg_no_trace("no series id given"))? as i64;
                let pgcon = create_connection(&ncc.node_config.cluster.database).await?;
                let mut rows = pgcon
                    .query("select name from channels where rowid = $1", &[&series])
                    .await
                    .err_conv()?;
                if let Some(row) = rows.pop() {
                    info!("{me} got a row {row:?}");
                    let name: String = row.get(0);
                    let channel = SfDbChannel::from_full(&ncc.node_config.cluster.backend, channel.series(), name);
                    info!("{me} return {channel:?}");
                    Ok(channel)
                } else {
                    info!("{me} nothing found");
                    Err(Error::with_msg_no_trace("can not find series"))
                }
            }
        } else {
            Err(Error::with_msg_no_trace(format!("{me}  bad input: {channel:?}")))
        }
    } else {
        Ok(channel)
    }
}
