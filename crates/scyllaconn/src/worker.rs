use crate::binwriteindex::BinWriteIndexEntry;
use crate::conn::create_scy_session_no_ks;
use crate::events2::events::ReadJobTrace;
use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use async_channel::Receiver;
use async_channel::Sender;
use daqbuf_err as err;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use daqbuf_series::SeriesId;
use futures_util::Future;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_bins::ContainerBins;
use netpod::log;
use netpod::ttl::RetentionTime;
use netpod::DtMs;
use netpod::ScyllaConfig;
use netpod::TsMs;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

macro_rules! info { ($($arg:expr),*) => ( if true { log::info!($($arg),*); } ); }
macro_rules! debug { ($($arg:expr),*) => ( if true { log::debug!($($arg),*); } ); }

const CONCURRENT_QUERIES_PER_WORKER: usize = 80;
const SCYLLA_WORKER_QUEUE_LEN: usize = 200;

autoerr::create_error_v1!(
    name(Error, "ScyllaWorker"),
    enum variants {
        ScyllaConnection(err::Error),
        Prepare(#[from] crate::events2::prepare::Error),
        Events(#[from] crate::events2::events::Error),
        Msp(#[from] crate::events2::msp::Error),
        ChannelSend,
        ChannelRecv,
        Join,
        Toplist(#[from] crate::accounting::toplist::Error),
        MissingKeyspaceConfig,
        CacheWriteF32(#[from] streams::timebin::cached::reader::Error),
        ScyllaType(#[from] scylla::deserialize::TypeCheckError),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
    },
);

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_: async_channel::SendError<T>) -> Self {
        Self::ChannelSend
    }
}

type ScySessTy = scylla::client::session::Session;

#[derive(Debug)]
struct ReadPrebinnedF32 {
    rt: RetentionTime,
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: core::ops::Range<u32>,
    tx: Sender<Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error>>,
}

#[derive(Debug)]
struct BinWriteIndexRead {
    rt1: RetentionTime,
    rt2: RetentionTime,
    series: SeriesId,
    pbp: PrebinnedPartitioning,
    msp: MspU32,
    lsp_min: u32,
    lsp_max: u32,
    tx: Sender<Result<VecDeque<BinWriteIndexEntry>, Error>>,
}

impl BinWriteIndexRead {
    async fn execute(self, stmts: &StmtsEvents, scy: &ScySessTy) {
        // TODO avoid the extra clone
        let tx = self.tx.clone();
        match self.execute_inner(stmts, scy).await {
            Ok(()) => {}
            Err(e) => {
                if tx.send(Err(e)).await.is_err() {
                    // TODO count for stats
                }
            }
        }
    }

    async fn execute_inner(self, stmts: &StmtsEvents, scy: &ScySessTy) -> Result<(), Error> {
        let params = (
            self.series.id() as i64,
            self.pbp.db_ix() as i16,
            self.msp.0 as i32,
            self.rt2.to_index_db_i32() as i16,
            self.lsp_min as i32,
            self.lsp_max as i32,
        );
        log::info!("execute {:?}", params);
        let res = scy
            .execute_iter(stmts.rt(&self.rt1).bin_write_index_read().clone(), params)
            .await?;
        let mut it = res.rows_stream::<(i16, i32, i32)>()?;
        let mut all = VecDeque::new();
        while let Some((rt, lsp, binlen)) = it.try_next().await? {
            let v = BinWriteIndexEntry {
                rt: rt as u16,
                lsp: lsp as u32,
                binlen: binlen as u32,
            };
            all.push_back(v);
        }
        self.tx.send(Ok(all)).await?;
        Ok(())
    }
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
    ReadPrebinnedF32(ReadPrebinnedF32),
    BinWriteIndexRead(BinWriteIndexRead),
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

    pub async fn read_prebinned_f32(
        &self,
        rt: RetentionTime,
        series: u64,
        bin_len: DtMs,
        msp: u64,
        offs: core::ops::Range<u32>,
    ) -> Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadPrebinnedF32(ReadPrebinnedF32 {
            rt,
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

    pub async fn bin_write_index_read(
        &self,
        rt1: RetentionTime,
        rt2: RetentionTime,
        series: SeriesId,
        pbp: PrebinnedPartitioning,
        msp: MspU32,
        lsp_min: u32,
        lsp_max: u32,
    ) -> Result<VecDeque<BinWriteIndexEntry>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = BinWriteIndexRead {
            rt1,
            rt2,
            series,
            pbp,
            msp,
            lsp_min,
            lsp_max,
            tx,
        };
        let job = Job::BinWriteIndexRead(job);
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
        let (tx, rx) = async_channel::bounded(SCYLLA_WORKER_QUEUE_LEN);
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
        debug!("scylla worker  prepare start");
        let stmts = StmtsEvents::new(kss.try_into().map_err(|_| Error::MissingKeyspaceConfig)?, &scy).await?;
        let stmts = Arc::new(stmts);
        // let stmts_cache = StmtsCache::new(kss[0], &scy).await?;
        // let stmts_cache = Arc::new(stmts_cache);
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
                    Job::WriteCacheF32(a, b, tx) => {
                        let _ = a;
                        let _ = b;
                        // let res = super::bincache::worker_write(series, bins, &stmts_cache, &scy).await;
                        let res = Err(streams::timebin::cached::reader::Error::TodoImpl);
                        if tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::ReadPrebinnedF32(job) => {
                        let res = super::bincache::worker_read(
                            job.rt,
                            job.series,
                            job.bin_len,
                            job.msp,
                            job.offs,
                            &stmts,
                            &scy,
                        )
                        .await;
                        if job.tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::BinWriteIndexRead(job) => job.execute(&stmts, &scy).await,
                }
            })
            .buffer_unordered(CONCURRENT_QUERIES_PER_WORKER)
            .for_each(|_| futures_util::future::ready(()))
            .await;
        info!("scylla worker finished");
        Ok(())
    }
}
