use crate::create_connection;
use crate::ErrConv;
use err::Error;
use netpod::Channel;
use netpod::NodeConfigCached;

// For sf-databuffer backend, given a Channel, try to complete the information if only id is given.
pub async fn sf_databuffer_fetch_channel_by_series(channel: Channel, ncc: &NodeConfigCached) -> Result<Channel, Error> {
    // TODO should not be needed at some point.
    if channel.backend().is_empty() || channel.name().is_empty() {
        let series = channel
            .series()
            .ok_or_else(|| Error::with_msg_no_trace("no series id given"))? as i64;
        let pgcon = create_connection(&ncc.node_config.cluster.database).await?;
        let mut rows = pgcon
            .query("select name from channels where rowid = $1", &[&series])
            .await
            .err_conv()?;
        if let Some(row) = rows.pop() {
            let name: String = row.get(0);
            let channel = Channel {
                series: channel.series,
                backend: ncc.node_config.cluster.backend.clone(),
                name,
            };
            Ok(channel)
        } else {
            Err(Error::with_msg_no_trace("can not find series"))
        }
    } else {
        Ok(channel)
    }
}
