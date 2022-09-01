pub mod bincache;
pub mod errconv;
pub mod events;

use err::Error;
use errconv::ErrConv;
use netpod::ScyllaConfig;
use scylla::Session as ScySession;
use std::sync::Arc;

pub async fn create_scy_session(scyconf: &ScyllaConfig) -> Result<Arc<ScySession>, Error> {
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .use_keyspace(&scyconf.keyspace, true)
        .build()
        .await
        .err_conv()?;
    let ret = Arc::new(scy);
    Ok(ret)
}
