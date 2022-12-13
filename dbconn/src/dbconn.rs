pub mod channelconfig;
pub mod scan;
pub mod search;

pub mod pg {
    pub use tokio_postgres::{Client, Error};
}

use err::Error;
use netpod::log::*;
use netpod::{Channel, Database, NodeConfigCached};
use netpod::{ScalarType, Shape};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::{Client, Client as PgClient, NoTls};

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

pub async fn create_connection(db_config: &Database) -> Result<Client, Error> {
    // TODO use a common already running worker pool for these queries:
    let d = db_config;
    let uri = format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, 5432, d.name);
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

pub async fn channel_exists(channel: &Channel, node_config: &NodeConfigCached) -> Result<bool, Error> {
    let cl = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl
        .query("select rowid from channels where name = $1::text", &[&channel.name])
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

pub struct TableSizes {
    pub sizes: Vec<(String, String)>,
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
    let mut sizes = TableSizes { sizes: vec![] };
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

pub async fn insert_channel(name: String, facility: i64, dbc: &Client) -> Result<(), Error> {
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

pub async fn find_series(channel: &Channel, pgclient: Arc<PgClient>) -> Result<(u64, ScalarType, Shape), Error> {
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
