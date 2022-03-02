use bytes::BytesMut;
use err::Error;
use netpod::log::*;
use std::os::unix::prelude::RawFd;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Once;
use tokio::sync::{mpsc, oneshot};

pub struct ReadTask {
    fd: RawFd,
    pos: u64,
    count: u64,
    rescell: oneshot::Sender<Result<ReadResult, Error>>,
}

pub struct ReadResult {
    pub buf: BytesMut,
    pub eof: bool,
}

/*
Async code must be able to interact with the Read3 system via async methods.
The async code must be able to enqueue a read in non-blocking fashion.
Since the queue of pending read requests must be bounded, this must be able to async-block.
*/
pub struct Read3 {
    jobs_tx: mpsc::Sender<ReadTask>,
    rtx: crossbeam::channel::Sender<mpsc::Receiver<ReadTask>>,
}

impl Read3 {
    pub fn get() -> &'static Self {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let (jtx, jrx) = mpsc::channel(32);
            let (rtx, rrx) = crossbeam::channel::bounded(16);
            let read3 = Read3 { jobs_tx: jtx, rtx };
            let b = Box::new(read3);
            let ptr = Box::into_raw(b);
            READ3.store(ptr, Ordering::SeqCst);
            let ptr = READ3.load(Ordering::SeqCst);
            let h = unsafe { &*ptr };
            if let Err(_) = h.rtx.send(jrx) {
                error!("Read3 INIT: can not enqueue main job reader");
            }
            for _ in 0..2 {
                let rrx = rrx.clone();
                tokio::task::spawn_blocking(move || h.read_worker(rrx));
            }
        });
        let ptr = READ3.load(Ordering::SeqCst);
        unsafe { &*ptr }
    }

    pub async fn read(&self, fd: RawFd, pos: u64, count: u64) -> Result<ReadResult, Error> {
        let (tx, rx) = oneshot::channel();
        let rt = ReadTask {
            fd,
            pos,
            count,
            rescell: tx,
        };
        match self.jobs_tx.send(rt).await {
            Ok(_) => match rx.await {
                Ok(res) => res,
                Err(e) => Err(Error::with_msg(format!("can not receive read task result: {e}"))),
            },
            Err(e) => Err(Error::with_msg(format!("can not send read job task: {e}"))),
        }
    }

    fn read_worker(&self, rrx: crossbeam::channel::Receiver<mpsc::Receiver<ReadTask>>) {
        'outer: loop {
            match rrx.recv() {
                Ok(mut jrx) => match jrx.blocking_recv() {
                    Some(rt) => match self.rtx.send(jrx) {
                        Ok(_) => {
                            let mut buf = BytesMut::with_capacity(rt.count as usize);
                            let mut writable = rt.count as usize;
                            let rr = unsafe {
                                loop {
                                    info!("do pread  fd {}  count {}  offset {}", rt.fd, writable, rt.pos);
                                    let ec = libc::pread(rt.fd, buf.as_mut_ptr() as _, writable, rt.pos as i64);
                                    if ec == -1 {
                                        let errno = *libc::__errno_location();
                                        if errno == libc::EINVAL {
                                            info!("pread  EOF  fd {}  count {}  offset {}", rt.fd, writable, rt.pos);
                                            let rr = ReadResult { buf, eof: true };
                                            break Ok(rr);
                                        } else {
                                            warn!(
                                                "pread  ERROR  errno {}  fd {}  count {}  offset {}",
                                                errno, rt.fd, writable, rt.pos
                                            );
                                            // TODO use a more structured error
                                            let e = Error::with_msg_no_trace(format!(
                                                "pread  ERROR  errno {}  fd {}  count {}  offset {}",
                                                errno, rt.fd, writable, rt.pos
                                            ));
                                            break Err(e);
                                        }
                                    } else if ec == 0 {
                                        info!("pread  EOF  fd {}  count {}  offset {}", rt.fd, writable, rt.pos);
                                        let rr = ReadResult { buf, eof: true };
                                        break Ok(rr);
                                    } else if ec > 0 {
                                        buf.set_len(ec as usize);
                                        if ec as usize > writable {
                                            error!(
                                                "pread  TOOLARGE  ec {}  fd {}  count {}  offset {}",
                                                ec, rt.fd, writable, rt.pos
                                            );
                                            break 'outer;
                                        }
                                        writable -= ec as usize;
                                        if writable == 0 {
                                            let rr = ReadResult { buf, eof: false };
                                            break Ok(rr);
                                        }
                                    } else {
                                        error!(
                                            "pread  UNEXPECTED  ec {}  fd {}  count {}  offset {}",
                                            ec, rt.fd, writable, rt.pos
                                        );
                                        break 'outer;
                                    }
                                }
                            };
                            match rt.rescell.send(rr) {
                                Ok(_) => {}
                                Err(_) => {
                                    error!("can not publish the read result");
                                    break 'outer;
                                }
                            }
                        }
                        Err(e) => {
                            error!("can not return the job receiver: {e}");
                            break 'outer;
                        }
                    },
                    None => break 'outer,
                },
                Err(e) => {
                    error!("read_worker sees: {e}");
                    break 'outer;
                }
            }
        }
    }
}

static READ3: AtomicPtr<Read3> = AtomicPtr::new(std::ptr::null_mut());
