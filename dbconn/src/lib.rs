use err::Error;
use netpod::log::*;
use netpod::{Channel, NodeConfigCached};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

pub mod scan;
pub mod search;

pub async fn delay_us(mu: u64) {
    tokio::time::sleep(Duration::from_micros(mu)).await;
}

pub async fn delay_io_short() {
    delay_us(1000).await;
}

pub async fn delay_io_medium() {
    delay_us(2000).await;
}

pub async fn create_connection(node_config: &NodeConfigCached) -> Result<Client, Error> {
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
    Ok(cl)
}

pub async fn create_connection_2(node_config: NodeConfigCached) -> Result<Client, Error> {
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
    Ok(cl)
}

pub async fn channel_exists(channel: &Channel, node_config: &NodeConfigCached) -> Result<bool, Error> {
    let cl = create_connection(node_config).await?;
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

pub async fn database_size(node_config: &NodeConfigCached) -> Result<u64, Error> {
    let cl = create_connection(node_config).await?;
    let rows = cl
        .query(
            "select pg_database_size($1::text)",
            &[&node_config.node_config.cluster.database.name],
        )
        .await?;
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
    let cl = create_connection(node_config).await?;
    let rows = cl.query(sql, &[]).await?;
    let mut sizes = TableSizes { sizes: vec![] };
    sizes.sizes.push((format!("table"), format!("size")));
    for row in rows {
        sizes.sizes.push((row.get(0), row.get(1)));
    }
    Ok(sizes)
}

pub async fn random_channel(node_config: &NodeConfigCached) -> Result<String, Error> {
    let sql = "select name from channels order by rowid limit 1 offset (random() * (select count(rowid) from channels))::bigint";
    let cl = create_connection(node_config).await?;
    let rows = cl.query(sql, &[]).await?;
    if rows.len() == 0 {
        Err(Error::with_msg("can not get random channel"))?;
    }
    Ok(rows[0].get(0))
}
