#[cfg(feature = "devread")]
pub mod generated;
#[cfg(not(feature = "devread"))]
pub mod generated {}
pub mod archeng;
pub mod events;
#[cfg(feature = "devread")]
pub mod parse;
#[cfg(not(feature = "devread"))]
pub mod parsestub;
pub mod storagemerge;
#[cfg(feature = "devread")]
#[cfg(test)]
pub mod test;
pub mod timed;

use std::sync::atomic::{AtomicUsize, Ordering};

use async_channel::Sender;
use err::Error;
use futures_core::Future;
use netpod::log::*;
#[cfg(not(feature = "devread"))]
pub use parsestub as parse;

fn unescape_archapp_msg(inp: &[u8], mut ret: Vec<u8>) -> Result<Vec<u8>, Error> {
    ret.clear();
    let mut esc = false;
    for &k in inp.iter() {
        if k == 0x1b {
            esc = true;
        } else if esc {
            if k == 0x1 {
                ret.push(0x1b);
            } else if k == 0x2 {
                ret.push(0xa);
            } else if k == 0x3 {
                ret.push(0xd);
            } else {
                return Err(Error::with_msg_no_trace("malformed escaped archapp message"));
            }
            esc = false;
        } else {
            ret.push(k);
        }
    }
    Ok(ret)
}

static CHANNEL_SEND_ERROR: AtomicUsize = AtomicUsize::new(0);

fn channel_send_error() {
    let c = CHANNEL_SEND_ERROR.fetch_add(1, Ordering::AcqRel);
    if c < 10 {
        error!("CHANNEL_SEND_ERROR {}", c);
    }
}

fn wrap_task<T, O1, O2>(task: T, tx: Sender<Result<O2, Error>>)
where
    T: Future<Output = Result<O1, Error>> + Send + 'static,
    O1: Send + 'static,
    O2: Send + 'static,
{
    let task = async move {
        match task.await {
            Ok(_) => {}
            Err(e) => {
                if let Err(_) = tx.send(Err(e)).await {
                    channel_send_error();
                }
            }
        }
    };
    taskrun::spawn(task);
}
