use crate::create_connection;
use async_channel::Receiver;
use async_channel::RecvError;
use async_channel::Sender;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::ChConf;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::Database;
use taskrun::tokio;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

#[derive(Debug, ThisError)]
#[cstm(name = "PgWorker")]
pub enum Error {
    Error(#[from] err::Error),
    ChannelSend,
    ChannelRecv,
    Join,
}

impl From<RecvError> for Error {
    fn from(_value: RecvError) -> Self {
        Self::ChannelRecv
    }
}

impl err::ToErr for Error {
    fn to_err(self) -> err::Error {
        err::Error::from_string(self)
    }
}

#[derive(Debug)]
enum Job {
    ChConfBestMatchingNameRange(String, String, NanoRange, Sender<Result<ChConf, Error>>),
    ChConfForSeries(String, u64, Sender<Result<ChConf, Error>>),
    InfoForSeriesIds(
        Vec<u64>,
        Sender<Result<Vec<Option<crate::channelinfo::ChannelInfo>>, crate::channelinfo::Error>>,
    ),
    SearchChannel(ChannelSearchQuery, Sender<Result<ChannelSearchResult, err::Error>>),
}

#[derive(Debug, Clone)]
pub struct PgQueue {
    tx: Sender<Job>,
}

impl PgQueue {
    pub async fn chconf_for_series(
        &self,
        backend: &str,
        series: u64,
    ) -> Result<Receiver<Result<ChConf, Error>>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ChConfForSeries(backend.into(), series, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        Ok(rx)
    }

    pub async fn chconf_best_matching_name_range_job(
        &self,
        backend: &str,
        name: &str,
        range: NanoRange,
    ) -> Result<Receiver<Result<ChConf, Error>>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ChConfBestMatchingNameRange(backend.into(), name.into(), range, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        Ok(rx)
    }

    pub async fn info_for_series_ids(
        &self,
        series_ids: Vec<u64>,
    ) -> Result<Receiver<Result<Vec<Option<crate::channelinfo::ChannelInfo>>, crate::channelinfo::Error>>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::InfoForSeriesIds(series_ids, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        Ok(rx)
    }

    pub async fn search_channel_scylla(
        &self,
        query: ChannelSearchQuery,
    ) -> Result<Result<ChannelSearchResult, err::Error>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::SearchChannel(query, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let ret = rx.recv().await?;
        Ok(ret)
    }
}

#[derive(Debug)]
pub struct PgWorker {
    rx: Receiver<Job>,
    pg: Client,
    pgjh: Option<JoinHandle<Result<(), err::Error>>>,
}

impl PgWorker {
    pub async fn new(pgconf: &Database) -> Result<(PgQueue, Self), Error> {
        let (tx, rx) = async_channel::bounded(64);
        let (pg, pgjh) = create_connection(pgconf).await?;
        let queue = PgQueue { tx };
        let worker = Self {
            rx,
            pg,
            pgjh: Some(pgjh),
        };
        Ok((queue, worker))
    }

    pub async fn work(self) -> Result<(), Error> {
        loop {
            let x = self.rx.recv().await;
            let job = match x {
                Ok(x) => x,
                Err(_) => {
                    error!("PgWorker can not receive from channel");
                    return Err(Error::ChannelRecv);
                }
            };
            match job {
                Job::ChConfBestMatchingNameRange(backend, name, range, tx) => {
                    let res =
                        crate::channelconfig::chconf_best_matching_for_name_and_range(&backend, &name, range, &self.pg)
                            .await;
                    if tx.send(res.map_err(Into::into)).await.is_err() {
                        // TODO count for stats
                    }
                }
                Job::ChConfForSeries(backend, series, tx) => {
                    let res = crate::channelconfig::chconf_for_series(&backend, series, &self.pg).await;
                    if tx.send(res.map_err(Into::into)).await.is_err() {
                        // TODO count for stats
                    }
                }
                Job::InfoForSeriesIds(ids, tx) => {
                    let res = crate::channelinfo::info_for_series_ids(&ids, &self.pg).await;
                    if tx.send(res.map_err(Into::into)).await.is_err() {
                        // TODO count for stats
                    }
                }
                Job::SearchChannel(query, tx) => {
                    let res = crate::search::search_channel_scylla(query, &self.pg).await;
                    if tx.send(res.map_err(Into::into)).await.is_err() {
                        // TODO count for stats
                    }
                }
            }
        }
    }

    pub async fn join(&mut self) -> Result<(), Error> {
        if let Some(jh) = self.pgjh.take() {
            jh.await.map_err(|_| Error::Join)?.map_err(Error::from)?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn close(&self) {
        self.rx.close();
    }
}
