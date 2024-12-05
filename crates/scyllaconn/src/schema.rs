use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use netpod::ttl::RetentionTime;
use netpod::ScyllaConfig;
use scylla::Session as ScySession;

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaSchema")]
pub enum Error {
    Scylla(#[from] scylla::transport::errors::QueryError),
}

pub async fn schema(rt: RetentionTime, scyco: &ScyllaConfig, scy: &ScySession) -> Result<(), Error> {
    let table = "binned_scalar_f32";
    let cql = format!(
        concat!("alter table {}.{}{}", " add lst float"),
        &scyco.keyspace,
        rt.table_prefix(),
        table
    );
    let _ = scy.query_unpaged(cql, ()).await;
    Ok(())
}
