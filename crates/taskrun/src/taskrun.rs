pub use tokio;

use crate::log::*;
use err::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::panic;
use std::sync::Arc;
use std::sync::Mutex;
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
    match RUNTIME.lock() {
        Ok(mut g) => match g.as_ref() {
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
                        panic!("can not create runtime {e}");
                    }
                };
                let a = Arc::new(res);
                *g = Some(a.clone());
                a
            }
            Some(g) => g.clone(),
        },
        Err(e) => {
            eprintln!("can not lock tracing init {e}");
            panic!("can not lock tracing init {e}");
        }
    }
}

pub fn run<T, E, F>(fut: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    let runtime = get_runtime();
    match tracing_init() {
        Ok(_) => {}
        Err(()) => {
            eprintln!("ERROR tracing: can not init");
        }
    }
    let res = runtime.block_on(fut);
    match res {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("ERROR catched: {e}");
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
            .with_writer(io::stderr)
            .with_timer(timer)
            .with_target(true)
            .with_ansi(false)
            .with_thread_names(true)
            .with_filter(filter);

        let reg = tracing_subscriber::registry();

        #[cfg(DISABLED_CONSOLE)]
        let reg = {
            let (console_layer, console_server) = console_subscriber::ConsoleLayer::builder().build();
            tokio::spawn(console_server.serve());
            reg.with(console_layer)
        };

        #[cfg(DISABLED_CONSOLE)]
        let reg = {
            let pid = std::process::id();
            // let cspn = format!("/tmp/daqbuffer.tokio.console.pid.{pid}");
            let console_layer = console_subscriber::ConsoleLayer::builder()
                // .retention(std::time::Duration::from_secs(10))
                .server_addr(([127, 0, 0, 1], 14571))
                // .server_addr(std::path::Path::new(&cspn))
                .spawn();
            // .build();

            // eprintln!("spawn console sever");
            // tokio::spawn(console_server.serve());
            reg.with(console_layer)
        };

        let reg = reg.with(fmt_layer);
        reg.try_init().map_err(|e| {
            eprintln!("can not initialize tracing layer: {e}");
            format!("{e}")
        })?;
    }
    #[cfg(DISABLED_LOKI)]
    // TODO tracing_loki seems not well composable, try open telemetry instead.
    if false {
        /*let fmt_layer = tracing_subscriber::fmt::Layer::new()
        .with_writer(io::stderr)
        .with_timer(timer)
        .with_target(true)
        .with_ansi(false)
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
    match INIT_TRACING_ONCE.lock() {
        Ok(mut initg) => {
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
                eprintln!("ERROR unknown tracing state");
                Err(())
            }
        }
        Err(e) => {
            eprintln!("can not lock tracing init {e}");
            Err(())
        }
    }
}

pub fn spawn<T>(task: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}
