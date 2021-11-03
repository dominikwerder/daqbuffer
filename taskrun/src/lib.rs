pub mod append;

use crate::log::*;
use err::Error;
use std::future::Future;
use std::panic;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

pub mod log {
    #[allow(unused_imports)]
    pub use tracing::{debug, error, info, trace, warn};
}

lazy_static::lazy_static! {
    static ref RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);
}

pub fn get_runtime() -> Arc<Runtime> {
    let mut g = RUNTIME.lock().unwrap();
    match g.as_ref() {
        None => {
            tracing_init();
            let res = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(12)
                .max_blocking_threads(256)
                .enable_all()
                .on_thread_start(|| {
                    let _old = panic::take_hook();
                    panic::set_hook(Box::new(move |info| {
                        let payload = if let Some(k) = info.payload().downcast_ref::<Error>() {
                            format!("{:?}", k)
                        }
                        else if let Some(k) = info.payload().downcast_ref::<String>() {
                            k.into()
                        }
                        else if let Some(&k) = info.payload().downcast_ref::<&str>() {
                            k.into()
                        }
                        else {
                            format!("unknown payload type")
                        };
                        error!(
                            "✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗     panicking\n{:?}\nLOCATION: {:?}\nPAYLOAD: {:?}\ninfo object: {:?}\nerr: {:?}",
                            Error::with_msg("catched panic in taskrun::run"),
                            info.location(),
                            info.payload(),
                            info,
                            payload,
                        );
                        //old(info);
                    }));
                })
                .build()
                .unwrap();
            let a = Arc::new(res);
            *g = Some(a.clone());
            a
        }
        Some(g) => g.clone(),
    }
}

pub fn run<T, F: std::future::Future<Output = Result<T, Error>>>(f: F) -> Result<T, Error> {
    let runtime = get_runtime();
    let res = runtime.block_on(async { f.await });
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("{:?}", e);
            Err(e)
        }
    }
}

lazy_static::lazy_static! {
    pub static ref INITMX: Mutex<u32> = Mutex::new(0);
}

pub fn tracing_init() {
    let mut g = INITMX.lock().unwrap();
    if *g == 0 {
        tracing_subscriber::fmt()
            .with_timer(tracing_subscriber::fmt::time::ChronoUtc::with_format(
                "%Y-%m-%dT%H:%M:%S%.3fZ".into(),
            ))
            //.with_timer(tracing_subscriber::fmt::time::uptime())
            .with_target(true)
            .with_thread_names(true)
            //.with_max_level(tracing::Level::INFO)
            .with_env_filter(tracing_subscriber::EnvFilter::new(
                [
                    "info",
                    "archapp::archeng=info",
                    "archapp::archeng::datablockstream=info",
                    "archapp::archeng::indextree=info",
                    "archapp::archeng::blockstream=info",
                    "archapp::archeng::ringbuf=info",
                    "archapp::archeng::backreadbuf=info",
                    "archapp::storagemerge=info",
                    "daqbuffer::test=trace",
                ]
                .join(","),
            ))
            .init();
        *g = 1;
        //warn!("tracing_init  done");
    }
}

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}

pub fn test_cluster() -> netpod::Cluster {
    let nodes = (0..3)
        .into_iter()
        .map(|id| netpod::Node {
            host: "localhost".into(),
            listen: "0.0.0.0".into(),
            port: 8360 + id as u16,
            port_raw: 8360 + id as u16 + 100,
            data_base_path: format!("../tmpdata/node{:02}", id).into(),
            cache_base_path: format!("../tmpdata/node{:02}", id).into(),
            ksprefix: "ks".into(),
            backend: "testbackend".into(),
            splits: None,
            archiver_appliance: None,
            channel_archiver: None,
        })
        .collect();
    netpod::Cluster {
        nodes,
        database: netpod::Database {
            name: "daqbuffer".into(),
            host: "localhost".into(),
            user: "daqbuffer".into(),
            pass: "daqbuffer".into(),
        },
        run_map_pulse_task: false,
        is_central_storage: false,
        file_io_buffer_size: Default::default(),
    }
}
