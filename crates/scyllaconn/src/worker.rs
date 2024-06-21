use crate::conn::create_scy_session_no_ks;
use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use async_channel::Receiver;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use items_0::Events;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::ScyllaConfig;
use netpod::TsMs;
use scylla::Session;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("ScyllaConnection({0})")]
    ScyllaConnection(err::Error),
    Prepare(#[from] crate::events2::prepare::Error),
    EventsQuery(#[from] crate::events::Error),
    Msp(#[from] crate::events2::msp::Error),
    ChannelSend,
    ChannelRecv,
    Join,
}

#[derive(Debug)]
enum Job {
    FindTsMsp(
        RetentionTime,
        // series-id
        u64,
        ScyllaSeriesRange,
        bool,
        Sender<Result<VecDeque<TsMs>, Error>>,
    ),
    ReadNextValues(ReadNextValues),
}

struct ReadNextValues {
    futgen: Box<
        dyn FnOnce(
                Arc<Session>,
                Arc<StmtsEvents>,
            ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>
            + Send,
    >,
    // fut: Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>,
    tx: Sender<Result<Box<dyn Events>, Error>>,
}

impl fmt::Debug for ReadNextValues {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "ReadNextValues {{ .. }}")
    }
}

#[derive(Debug, Clone)]
pub struct ScyllaQueue {
    tx: Sender<Job>,
}

impl ScyllaQueue {
    pub async fn find_ts_msp(
        &self,
        rt: RetentionTime,
        series: u64,
        range: ScyllaSeriesRange,
        bck: bool,
    ) -> Result<VecDeque<TsMs>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::FindTsMsp(rt, series, range, bck, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn read_next_values<F>(&self, futgen: F) -> Result<Box<dyn Events>, Error>
    where
        F: FnOnce(
                Arc<Session>,
                Arc<StmtsEvents>,
            ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>
            + Send
            + 'static,
    {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadNextValues(ReadNextValues {
            futgen: Box::new(futgen),
            tx,
        });
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }
}

#[derive(Debug)]
pub struct ScyllaWorker {
    rx: Receiver<Job>,
    scyconf_st: ScyllaConfig,
    scyconf_mt: ScyllaConfig,
    scyconf_lt: ScyllaConfig,
}

impl ScyllaWorker {
    pub async fn new(
        scyconf_st: ScyllaConfig,
        scyconf_mt: ScyllaConfig,
        scyconf_lt: ScyllaConfig,
    ) -> Result<(ScyllaQueue, Self), Error> {
        let (tx, rx) = async_channel::bounded(64);
        let queue = ScyllaQueue { tx };
        let worker = Self {
            rx,
            scyconf_st,
            scyconf_mt,
            scyconf_lt,
        };
        Ok((queue, worker))
    }

    pub async fn work(self) -> Result<(), Error> {
        let scy = create_scy_session_no_ks(&self.scyconf_st)
            .await
            .map_err(Error::ScyllaConnection)?;
        let scy = Arc::new(scy);
        let kss = [
            self.scyconf_st.keyspace.as_str(),
            self.scyconf_mt.keyspace.as_str(),
            self.scyconf_lt.keyspace.as_str(),
        ];
        let stmts = StmtsEvents::new(kss.try_into().unwrap(), &scy).await?;
        let stmts = Arc::new(stmts);
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
                Job::FindTsMsp(rt, series, range, bck, tx) => {
                    let res = crate::events2::msp::find_ts_msp(&rt, series, range, bck, &stmts, &scy).await;
                    if tx.send(res.map_err(Into::into)).await.is_err() {
                        // TODO count for stats
                    }
                }
                Job::ReadNextValues(job) => {
                    let fut = (job.futgen)(scy.clone(), stmts.clone());
                    let res = fut.await;
                    if job.tx.send(res.map_err(Into::into)).await.is_err() {
                        // TODO count for stats
                    }
                }
            }
        }
    }
}
