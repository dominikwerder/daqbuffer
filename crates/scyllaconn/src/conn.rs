use crate::errconv::ErrConv;
use err::Error;
use netpod::ScyllaConfig;
use scylla::execution_profile::ExecutionProfileBuilder;
use scylla::statement::Consistency;
use scylla::Session as ScySession;
use std::sync::Arc;

pub async fn create_scy_session(scyconf: &ScyllaConfig) -> Result<Arc<ScySession>, Error> {
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .use_keyspace(&scyconf.keyspace, true)
        .default_execution_profile_handle(
            ExecutionProfileBuilder::default()
                .consistency(Consistency::LocalOne)
                .build()
                .into_handle(),
        )
        .build()
        .await
        .err_conv()?;
    let ret = Arc::new(scy);
    Ok(ret)
}
