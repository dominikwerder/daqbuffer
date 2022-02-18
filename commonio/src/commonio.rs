pub mod ringbuf;

use async_channel::Sender;
use err::{ErrStr, Error};
use futures_util::StreamExt;
use items::eventsitem::EventsItem;
use items::{Sitemty, StatsItem, StreamItem};
use netpod::log::*;
use netpod::{DiskStats, OpenStats, ReadExactStats, ReadStats, SeekStats};
use std::fmt;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const LOG_IO: bool = true;
const STATS_IO: bool = true;

pub async fn tokio_read(path: impl AsRef<Path>) -> Result<Vec<u8>, Error> {
    let path = path.as_ref();
    tokio::fs::read(path)
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("Can not open {path:?} {e:?}")))
}

pub struct StatsChannel {
    chn: Sender<Sitemty<EventsItem>>,
}

impl fmt::Debug for StatsChannel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StatsChannel").finish()
    }
}

impl StatsChannel {
    pub fn new(chn: Sender<Sitemty<EventsItem>>) -> Self {
        Self { chn }
    }

    pub fn dummy() -> Self {
        let (tx, rx) = async_channel::bounded(2);
        taskrun::spawn(async move {
            let mut rx = rx;
            while let Some(_) = rx.next().await {}
        });
        Self::new(tx)
    }

    pub async fn send(&self, item: StatsItem) -> Result<(), Error> {
        Ok(self.chn.send(Ok(StreamItem::Stats(item))).await.errstr()?)
    }
}

impl Clone for StatsChannel {
    fn clone(&self) -> Self {
        Self { chn: self.chn.clone() }
    }
}

/*
struct ReadExactWrap<'a> {
    fut: &'a mut dyn Future<Output = io::Result<usize>>,
}

trait TimedIo {
    fn read_exact<'a, F>(&'a mut self, buf: &'a mut [u8]) -> ReadExactWrap
    where
        Self: Unpin;
}

impl TimedIo for File {
    fn read_exact<'a, F>(&'a mut self, buf: &'a mut [u8]) -> ReadExactWrap
    where
        Self: Unpin,
    {
        let fut = tokio::io::AsyncReadExt::read_exact(self, buf);
        ReadExactWrap { fut: Box::pin(fut) }
    }
}
*/

static CHANNEL_SEND_ERROR: AtomicUsize = AtomicUsize::new(0);

fn channel_send_error() {
    let c = CHANNEL_SEND_ERROR.fetch_add(1, Ordering::AcqRel);
    if c < 10 {
        error!("CHANNEL_SEND_ERROR {}", c);
    }
}

pub async fn open_read(path: PathBuf, stats: &StatsChannel) -> io::Result<File> {
    let ts1 = Instant::now();
    let res = OpenOptions::new().read(true).open(path).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1);
    if LOG_IO {
        let dt = dt.as_secs_f64() * 1e3;
        debug!("timed open_read  dt: {:.3} ms", dt);
    }
    if STATS_IO {
        if let Err(_) = stats
            .send(StatsItem::DiskStats(DiskStats::OpenStats(OpenStats::new(
                ts2.duration_since(ts1),
            ))))
            .await
        {
            channel_send_error();
        }
    }
    res
}

pub async fn seek(file: &mut File, pos: SeekFrom, stats: &StatsChannel) -> io::Result<u64> {
    let ts1 = Instant::now();
    let res = file.seek(pos).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1);
    if LOG_IO {
        let dt = dt.as_secs_f64() * 1e3;
        debug!("timed seek  dt: {:.3} ms", dt);
    }
    if STATS_IO {
        if let Err(_) = stats
            .send(StatsItem::DiskStats(DiskStats::SeekStats(SeekStats::new(
                ts2.duration_since(ts1),
            ))))
            .await
        {
            channel_send_error();
        }
    }
    res
}

pub async fn read(file: &mut File, buf: &mut [u8], stats: &StatsChannel) -> io::Result<usize> {
    let ts1 = Instant::now();
    let res = file.read(buf).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1);
    if LOG_IO {
        let dt = dt.as_secs_f64() * 1e3;
        debug!("timed read  dt: {:.3} ms  res: {:?}", dt, res);
    }
    if STATS_IO {
        if let Err(_) = stats
            .send(StatsItem::DiskStats(DiskStats::ReadStats(ReadStats::new(
                ts2.duration_since(ts1),
            ))))
            .await
        {
            channel_send_error();
        }
    }
    res
}

pub async fn read_exact(file: &mut File, buf: &mut [u8], stats: &StatsChannel) -> io::Result<usize> {
    let ts1 = Instant::now();
    let res = file.read_exact(buf).await;
    let ts2 = Instant::now();
    let dt = ts2.duration_since(ts1);
    if LOG_IO {
        let dt = dt.as_secs_f64() * 1e3;
        debug!("timed read_exact  dt: {:.3} ms  res: {:?}", dt, res);
    }
    if STATS_IO {
        if let Err(_) = stats
            .send(StatsItem::DiskStats(DiskStats::ReadExactStats(ReadExactStats::new(
                ts2.duration_since(ts1),
            ))))
            .await
        {
            channel_send_error();
        };
    }
    res
}
