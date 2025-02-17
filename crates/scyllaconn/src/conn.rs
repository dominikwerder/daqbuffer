use crate::errconv::ErrConv;
use daqbuf_err as err;
use err::Error;
use netpod::log::*;
use netpod::ScyllaConfig;
use scylla::execution_profile::ExecutionProfileBuilder;
use scylla::statement::Consistency;
use scylla::Session as ScySession;
use std::sync::Arc;

pub async fn create_scy_session(scyconf: &ScyllaConfig) -> Result<Arc<ScySession>, Error> {
    let scy = create_scy_session_no_ks(scyconf).await?;
    scy.use_keyspace(&scyconf.keyspace, true)
        .await
        .map_err(Error::from_string)?;
    let ret = Arc::new(scy);
    Ok(ret)
}

pub async fn create_scy_session_no_ks(scyconf: &ScyllaConfig) -> Result<ScySession, Error> {
    info!("creating scylla connection");
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyconf.hosts)
        .default_execution_profile_handle(
            ExecutionProfileBuilder::default()
                .consistency(Consistency::LocalOne)
                .build()
                .into_handle(),
        )
        .build()
        .await
        .err_conv()?;
    Ok(scy)
}
