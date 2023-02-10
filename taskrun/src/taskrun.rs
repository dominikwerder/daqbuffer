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

static INIT_TRACING_ONCE: Mutex<usize> = Mutex::new(0);
static RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);

pub fn get_runtime() -> Arc<Runtime> {
    get_runtime_opts(24, 128)
}

#[allow(unused)]
fn on_thread_start() {
    let old = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let payload = if let Some(k) = info.payload().downcast_ref::<Error>() {
            format!("{:?}", k)
        } else if let Some(k) = info.payload().downcast_ref::<String>() {
            k.into()
        } else if let Some(&k) = info.payload().downcast_ref::<&str>() {
            k.into()
        } else {
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
        if false {
            old(info);
        }
    }));
}

pub fn get_runtime_opts(nworkers: usize, nblocking: usize) -> Arc<Runtime> {
    let mut g = RUNTIME.lock().unwrap();
    match g.as_ref() {
        None => {
            let res = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(nworkers)
                .max_blocking_threads(nblocking)
                .enable_all()
                .on_thread_start(on_thread_start)
                .build();
            let res = match res {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("ERROR {e}");
                    panic!();
                }
            };
            let a = Arc::new(res);
            *g = Some(a.clone());
            a
        }
        Some(g) => g.clone(),
    }
}

pub fn run<T, F>(fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    let runtime = get_runtime();
    match tracing_init() {
        Ok(_) => {}
        Err(e) => {
            eprintln!("TRACING: {e:?}");
        }
    }
    let res = runtime.block_on(async { fut.await });
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("Catched: {:?}", e);
            Err(e)
        }
    }
}

fn tracing_init_inner() -> Result<(), Error> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;
    let fmtstr = "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z";
    let timer = tracing_subscriber::fmt::time::UtcTime::new(
        time::format_description::parse(fmtstr).map_err(|e| format!("{e}"))?,
    );
    if true {
        let filter = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(tracing::metadata::LevelFilter::INFO.into())
            .from_env()
            .map_err(|e| Error::with_msg_no_trace(format!("can not build tracing env filter {e}")))?;
        let fmt_layer = tracing_subscriber::fmt::Layer::new()
            .with_timer(timer)
            .with_target(true)
            .with_thread_names(true)
            .with_filter(filter);
        let z = tracing_subscriber::registry().with(fmt_layer);
        #[cfg(CONSOLE)]
        {
            let console_layer = console_subscriber::spawn();
            let z = z.with(console_layer);
        }
        z.try_init().map_err(|e| format!("{e}"))?;
    }
    #[cfg(DISABLED_LOKI)]
    // TODO tracing_loki seems not well composable, try open telemetry instead.
    if false {
        /*let fmt_layer = tracing_subscriber::fmt::Layer::new()
        .with_timer(timer)
        .with_target(true)
        .with_thread_names(true)
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());*/
        let url = "http://[::1]:6947";
        //let url = "http://127.0.0.1:6947";
        //let url = "http://[::1]:6132";
        let (loki_layer, loki_task) = tracing_loki::layer(
            tracing_loki::url::Url::parse(url)?,
            vec![(format!("daqbuffer"), format!("dev"))].into_iter().collect(),
            [].into(),
        )
        .map_err(|e| format!("{e}"))?;
        //let loki_layer = loki_layer.with_filter(log_filter);
        eprintln!("MADE LAYER");
        tracing_subscriber::registry()
            //.with(fmt_layer)
            .with(loki_layer)
            //.try_init()
            //.map_err(|e| format!("{e}"))?;
            .init();
        eprintln!("REGISTERED");
        if true {
            tokio::spawn(loki_task);
            eprintln!("SPAWNED TASK");
        }
        eprintln!("INFO LOKI");
    }
    Ok(())
}

pub fn tracing_init() -> Result<(), ()> {
    let mut initg = INIT_TRACING_ONCE.lock().unwrap();
    if *initg == 0 {
        match tracing_init_inner() {
            Ok(_) => {
                *initg = 1;
            }
            Err(e) => {
                *initg = 2;
                eprintln!("tracing_init_inner gave error {e}");
            }
        }
        Ok(())
    } else if *initg == 1 {
        Ok(())
    } else {
        eprintln!("ERROR Unknown tracing state");
        Err(())
    }
}

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}
