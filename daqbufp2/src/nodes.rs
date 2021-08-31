use crate::spawn_test_hosts;
use err::Error;
use netpod::{Cluster, Database, Node};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub struct RunningHosts {
    pub cluster: Cluster,
    _jhs: Vec<JoinHandle<Result<(), Error>>>,
}

lazy_static::lazy_static! {
    static ref HOSTS_RUNNING: Mutex<Option<Arc<RunningHosts>>> = Mutex::new(None);
}

pub fn require_test_hosts_running() -> Result<Arc<RunningHosts>, Error> {
    let mut g = HOSTS_RUNNING.lock().unwrap();
    match g.as_ref() {
        None => {
            let cluster = taskrun::test_cluster();
            let jhs = spawn_test_hosts(cluster.clone());
            let ret = RunningHosts {
                cluster: cluster.clone(),
                _jhs: jhs,
            };
            let a = Arc::new(ret);
            *g = Some(a.clone());
            Ok(a)
        }
        Some(gg) => Ok(gg.clone()),
    }
}
