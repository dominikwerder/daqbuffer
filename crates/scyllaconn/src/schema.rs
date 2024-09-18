use err::thiserror;
use err::ThisError;
use netpod::ttl::RetentionTime;
use netpod::ScyllaConfig;
use scylla::Session as ScySession;

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaSchema")]
pub enum Error {
    Scylla,
}

pub async fn schema(rt: RetentionTime, scyco: &ScyllaConfig, scy: &ScySession) -> Result<(), Error> {
    todo!()
}
