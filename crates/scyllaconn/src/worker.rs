use crate::conn::create_scy_session_no_ks;
use crate::events::ReadJobTrace;
use crate::events2::prepare::StmtsCache;
use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use async_channel::Receiver;
use async_channel::Sender;
use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::StreamExt;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_bins::ContainerBins;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::DtMs;
use netpod::ScyllaConfig;
use netpod::TsMs;
use scylla::Session;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaWorker")]
pub enum Error {
    ScyllaConnection(err::Error),
    Prepare(#[from] crate::events2::prepare::Error),
    EventsQuery(#[from] crate::events::Error),
    Msp(#[from] crate::events2::msp::Error),
    ChannelSend,
    ChannelRecv,
    Join,
    Toplist(#[from] crate::accounting::toplist::Error),
    MissingKeyspaceConfig,
    CacheWriteF32(#[from] streams::timebin::cached::reader::Error),
    Schema(#[from] crate::schema::Error),
}

#[derive(Debug)]
struct ReadCacheF32 {
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: core::ops::Range<u32>,
    tx: Sender<Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error>>,
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
    AccountingReadTs(
        RetentionTime,
        TsMs,
        Sender<Result<crate::accounting::toplist::UsageData, crate::accounting::toplist::Error>>,
    ),
    WriteCacheF32(
        u64,
        ContainerBins<f32, f32>,
        Sender<Result<(), streams::timebin::cached::reader::Error>>,
    ),
    ReadCacheF32(ReadCacheF32),
}

struct ReadNextValues {
    futgen: Box<
        dyn FnOnce(
                Arc<Session>,
                Arc<StmtsEvents>,
                ReadJobTrace,
            ) -> Pin<
                Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>,
            > + Send,
    >,
    tx: Sender<Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>>,
    jobtrace: ReadJobTrace,
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

    pub async fn read_next_values<F>(
        &self,
        futgen: F,
        jobtrace: ReadJobTrace,
    ) -> Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>
    where
        F: FnOnce(
                Arc<Session>,
                Arc<StmtsEvents>,
                ReadJobTrace,
            ) -> Pin<
                Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>,
            > + Send
            + 'static,
    {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadNextValues(ReadNextValues {
            futgen: Box::new(futgen),
            tx,
            jobtrace,
        });
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn accounting_read_ts(
        &self,
        rt: RetentionTime,
        ts: TsMs,
    ) -> Result<crate::accounting::toplist::UsageData, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::AccountingReadTs(rt, ts, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn write_cache_f32(
        &self,
        series: u64,
        bins: ContainerBins<f32, f32>,
    ) -> Result<(), streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::WriteCacheF32(series, bins, tx);
        self.tx
            .send(job)
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn read_cache_f32(
        &self,
        series: u64,
        bin_len: DtMs,
        msp: u64,
        offs: core::ops::Range<u32>,
    ) -> Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadCacheF32(ReadCacheF32 {
            series,
            bin_len,
            msp,
            offs,
            tx,
        });
        self.tx
            .send(job)
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
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
        let (tx, rx) = async_channel::bounded(200);
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
        crate::schema::schema(RetentionTime::Short, &self.scyconf_st, &scy).await?;
        crate::schema::schema(RetentionTime::Medium, &self.scyconf_mt, &scy).await?;
        crate::schema::schema(RetentionTime::Long, &self.scyconf_lt, &scy).await?;
        let kss = [
            self.scyconf_st.keyspace.as_str(),
            self.scyconf_mt.keyspace.as_str(),
            self.scyconf_lt.keyspace.as_str(),
        ];
        debug!("scylla worker  prepare start");
        let stmts = StmtsEvents::new(kss.try_into().map_err(|_| Error::MissingKeyspaceConfig)?, &scy).await?;
        let stmts = Arc::new(stmts);
        let stmts_cache = StmtsCache::new(kss[0], &scy).await?;
        let stmts_cache = Arc::new(stmts_cache);
        debug!("scylla worker  prepare done");
        self.rx
            .map(|job| async {
                match job {
                    Job::FindTsMsp(rt, series, range, bck, tx) => {
                        let res = crate::events2::msp::find_ts_msp(&rt, series, range, bck, &stmts, &scy).await;
                        if tx.send(res.map_err(Into::into)).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::ReadNextValues(job) => {
                        let fut = (job.futgen)(scy.clone(), stmts.clone(), job.jobtrace);
                        let res = fut.await;
                        if job.tx.send(res.map_err(Into::into)).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::AccountingReadTs(rt, ts, tx) => {
                        let ks = match &rt {
                            RetentionTime::Short => &self.scyconf_st.keyspace,
                            RetentionTime::Medium => &self.scyconf_mt.keyspace,
                            RetentionTime::Long => &self.scyconf_lt.keyspace,
                        };
                        let res = crate::accounting::toplist::read_ts(&ks, rt, ts, &scy).await;
                        if tx.send(res.map_err(Into::into)).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::WriteCacheF32(series, bins, tx) => {
                        let res = super::bincache::worker_write(series, bins, &stmts_cache, &scy).await;
                        if tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::ReadCacheF32(job) => {
                        let res = super::bincache::worker_read(
                            job.series,
                            job.bin_len,
                            job.msp,
                            job.offs,
                            &stmts_cache,
                            &scy,
                        )
                        .await;
                        if job.tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                }
            })
            .buffer_unordered(80)
            .for_each(|_| futures_util::future::ready(()))
            .await;
        info!("scylla worker finished");
        Ok(())
    }
}
