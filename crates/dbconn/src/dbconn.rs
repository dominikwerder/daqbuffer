pub mod channelconfig;
pub mod query;
pub mod scan;
pub mod search;

pub mod pg {
    pub use tokio_postgres::Client;
    pub use tokio_postgres::Error;
    pub use tokio_postgres::NoTls;
    pub use tokio_postgres::Statement;
}

use err::anyhow;
use err::thiserror;
use err::Error;
use err::Res2;
use err::ThisError;
use netpod::log::*;
use netpod::Database;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::TableSizes;
use pg::Client as PgClient;
use pg::NoTls;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use taskrun::tokio;

trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T> ErrConv<T> for Result<T, tokio_postgres::Error> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg(e.to_string())),
        }
    }
}

impl<T, A> ErrConv<T> for Result<T, async_channel::SendError<A>> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg(e.to_string())),
        }
    }
}

pub async fn delay_us(mu: u64) {
    tokio::time::sleep(Duration::from_micros(mu)).await;
}

pub async fn delay_io_short() {
    delay_us(1000).await;
}

pub async fn delay_io_medium() {
    delay_us(2000).await;
}

pub async fn create_connection(db_config: &Database) -> Result<PgClient, Error> {
    // TODO use a common already running worker pool for these queries:
    let d = db_config;
    let uri = format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name);
    let (cl, conn) = tokio_postgres::connect(&uri, NoTls)
        .await
        .map_err(|e| format!("Can not connect to database: {e:?}"))
        //.errconv()
        ?;
    // TODO monitor connection drop.
    let _cjh = tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e);
        }
        Ok::<_, Error>(())
    });
    Ok(cl)
}

pub async fn channel_exists(channel_name: &str, node_config: &NodeConfigCached) -> Result<bool, Error> {
    let cl = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl
        .query("select rowid from channels where name = $1::text", &[&channel_name])
        .await
        .err_conv()?;
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

pub async fn database_size(node_config: &NodeConfigCached) -> Result<u64, Error> {
    let cl = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl
        .query(
            "select pg_database_size($1::text)",
            &[&node_config.node_config.cluster.database.name],
        )
        .await
        .err_conv()?;
    if rows.len() == 0 {
        Err(Error::with_msg("could not get database size"))?;
    }
    let size: i64 = rows[0].get(0);
    let size = size as u64;
    Ok(size)
}

pub async fn table_sizes(node_config: &NodeConfigCached) -> Result<TableSizes, Error> {
    let sql = format!(
        "{} {} {} {} {} {} {}",
        "SELECT nspname || '.' || relname AS relation, pg_size_pretty(pg_total_relation_size(C.oid)) AS total_size",
        "FROM pg_class C",
        "LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)",
        "WHERE nspname NOT IN ('pg_catalog', 'information_schema')",
        "AND C.relkind <> 'i'",
        "AND nspname !~ '^pg_toast'",
        "ORDER BY pg_total_relation_size(C.oid) DESC LIMIT 20",
    );
    let sql = sql.as_str();
    let cl = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl.query(sql, &[]).await.err_conv()?;
    let mut sizes = TableSizes { sizes: Vec::new() };
    sizes.sizes.push((format!("table"), format!("size")));
    for row in rows {
        sizes.sizes.push((row.get(0), row.get(1)));
    }
    Ok(sizes)
}

pub async fn random_channel(node_config: &NodeConfigCached) -> Result<String, Error> {
    let sql = "select name from channels order by rowid limit 1 offset (random() * (select count(rowid) from channels))::bigint";
    let cl = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl.query(sql, &[]).await.err_conv()?;
    if rows.len() == 0 {
        Err(Error::with_msg("can not get random channel"))?;
    }
    Ok(rows[0].get(0))
}

pub async fn insert_channel(name: String, facility: i64, dbc: &PgClient) -> Result<(), Error> {
    let rows = dbc
        .query(
            "select count(rowid) from channels where facility = $1 and name = $2",
            &[&facility, &name],
        )
        .await
        .err_conv()?;
    if rows[0].get::<_, i64>(0) == 0 {
        let sql =
            concat!("insert into channels (facility, name) values ($1, $2) on conflict (facility, name) do nothing");
        dbc.query(sql, &[&facility, &name]).await.err_conv()?;
    }
    Ok(())
}

// Currently only for scylla type backends
pub async fn find_series(channel: &SfDbChannel, pgclient: Arc<PgClient>) -> Result<(u64, ScalarType, Shape), Error> {
    info!("find_series  channel {:?}", channel);
    let rows = if let Some(series) = channel.series() {
        let q = "select series, facility, channel, scalar_type, shape_dims from series_by_channel where series = $1";
        pgclient.query(q, &[&(series as i64)]).await.err_conv()?
    } else {
        let q = "select series, facility, channel, scalar_type, shape_dims from series_by_channel where facility = $1 and channel = $2";
        pgclient
            .query(q, &[&channel.backend(), &channel.name()])
            .await
            .err_conv()?
    };
    if rows.len() < 1 {
        return Err(Error::with_public_msg_no_trace(format!(
            "No series found for {channel:?}"
        )));
    }
    if rows.len() > 1 {
        error!("Multiple series found for {channel:?}");
        return Err(Error::with_public_msg_no_trace(
            "Multiple series found for channel, can not return data for ambiguous series",
        ));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::with_public_msg_no_trace(format!("can not find series for channel")))?;
    let series = row.get::<_, i64>(0) as u64;
    let _facility: String = row.get(1);
    let _channel: String = row.get(2);
    let a: i32 = row.get(3);
    let scalar_type = ScalarType::from_scylla_i32(a)?;
    let a: Vec<i32> = row.get(4);
    let shape = Shape::from_scylla_shape_dims(&a)?;
    Ok((series, scalar_type, shape))
}

// Currently only for sf-databuffer type backends
// Note: we currently treat the channels primary key as series-id for sf-databuffer type backends.
pub async fn find_series_sf_databuffer(channel: &SfDbChannel, pgclient: Arc<PgClient>) -> Res2<u64> {
    debug!("find_series_sf_databuffer  {:?}", channel);
    let sql = "select rowid from facilities where name = $1";
    let rows = pgclient.query(sql, &[&channel.backend()]).await.err_conv()?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no backend for {channel:?}"))?;
    let backend_id: i64 = row.get(0);
    let sql = "select rowid from channels where facility = $1 and name = $2";
    let rows = pgclient.query(sql, &[&backend_id, &channel.name()]).await.err_conv()?;
    if rows.len() < 1 {
        return Err(anyhow::anyhow!("No series found for {channel:?}"));
    }
    if rows.len() > 1 {
        return Err(anyhow::anyhow!("Multiple series found for {channel:?}"));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No series found for {channel:?}"))?;
    let series = row.get::<_, i64>(0) as u64;
    Ok(series)
}

#[derive(Debug, ThisError, Serialize)]
pub enum FindChannelError {
    UnknownBackend,
    BadSeriesId,
    NoFound,
    MultipleFound,
    Database(String),
}

// On sf-databuffer, the channel name identifies the series. But we can also have a series id.
// This function is used if the request provides only the series-id, but no name.
pub async fn find_sf_channel_by_series(
    channel: SfDbChannel,
    pgclient: Arc<PgClient>,
) -> Result<SfDbChannel, FindChannelError> {
    debug!("find_sf_channel_by_series  {:?}", channel);
    let series = channel.series().ok_or_else(|| FindChannelError::BadSeriesId)?;
    let sql = "select rowid from facilities where name = $1";
    let rows = pgclient
        .query(sql, &[&channel.backend()])
        .await
        .map_err(|e| FindChannelError::Database(e.to_string()))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| FindChannelError::UnknownBackend)?;
    let backend_id: i64 = row.get(0);
    let sql = "select name from channels where facility = $1 and rowid = $2";
    let rows = pgclient
        .query(sql, &[&backend_id, &(series as i64)])
        .await
        .map_err(|e| FindChannelError::Database(e.to_string()))?;
    if rows.len() > 1 {
        return Err(FindChannelError::MultipleFound);
    }
    if let Some(row) = rows.into_iter().next() {
        let name = row.get::<_, String>(0);
        let channel = SfDbChannel::from_full(channel.backend(), channel.series(), name);
        Ok(channel)
    } else {
        return Err(FindChannelError::NoFound);
    }
}
