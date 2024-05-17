use crate::conn::create_scy_session_no_ks;
use async_channel::Receiver;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::ScyllaConfig;
use scylla::Session;

#[derive(Debug, ThisError)]
pub enum Error {
    Error(#[from] err::Error),
    ChannelSend,
    ChannelRecv,
    Join,
}

impl err::ToErr for Error {
    fn to_err(self) -> err::Error {
        err::Error::from_string(self)
    }
}

#[derive(Debug)]
enum Job {
    JobA(String, Sender<Result<String, Error>>),
}

#[derive(Debug, Clone)]
pub struct ScyllaQueue {
    tx: Sender<Job>,
}

impl ScyllaQueue {
    pub async fn job_a(&self, backend: &str) -> Result<Receiver<Result<String, Error>>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::JobA(backend.into(), tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        Ok(rx)
    }
}

#[derive(Debug)]
pub struct ScyllaWorker {
    rx: Receiver<Job>,
    scy: Session,
    // pgjh: Option<JoinHandle<Result<(), err::Error>>>,
}

impl ScyllaWorker {
    pub async fn new(scyconf: &ScyllaConfig) -> Result<(ScyllaQueue, Self), Error> {
        let (tx, rx) = async_channel::bounded(64);
        let scy = create_scy_session_no_ks(scyconf).await?;
        let queue = ScyllaQueue { tx };
        let worker = Self {
            rx,
            scy,
            // pgjh: Some(pgjh),
        };
        Ok((queue, worker))
    }

    pub async fn work(self) -> Result<(), Error> {
        loop {
            let x = self.rx.recv().await;
            let job = match x {
                Ok(x) => x,
                Err(_) => {
                    error!("ScyllaWorker can not receive from channel");
                    return Err(Error::ChannelRecv);
                }
            };
            match job {
                Job::JobA(backend, tx) => {
                    let res = Ok::<_, Error>(backend);
                    if tx.send(res.map_err(Into::into)).await.is_err() {
                        // TODO count for stats
                    }
                }
            }
        }
    }
}
