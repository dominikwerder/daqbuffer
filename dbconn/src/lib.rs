use err::Error;
use netpod::log::*;
use netpod::{Channel, NodeConfigCached};
use tokio_postgres::NoTls;

pub async fn channel_exists(channel: &Channel, node_config: &NodeConfigCached) -> Result<bool, Error> {
    let d = &node_config.node_config.cluster.database;
    let uri = format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, 5432, d.name);
    let (cl, conn) = tokio_postgres::connect(&uri, NoTls).await?;
    // TODO monitor connection drop.
    let _cjh = tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e);
        }
        Ok::<_, Error>(())
    });
    let rows = cl
        .query("select rowid from channels where name = $1::text", &[&channel.name])
        .await?;
    debug!("channel_exists  {} rows", rows.len());
    for row in rows {
        debug!(
            "  db on channel search: {:?}  {:?}  {:?}",
            row,
            row.columns(),
            row.get::<_, i64>(0)
        );
    }
    Ok(true)
}
