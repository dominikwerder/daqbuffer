use commonio::{read, seek, StatsChannel};
use err::Error;
use netpod::log::*;
use std::borrow::BorrowMut;
use std::fmt;
use std::io::SeekFrom;
use tokio::fs::File;

pub struct BackReadBuf<F> {
    file: F,
    buf: Vec<u8>,
    abs: u64,
    wp: usize,
    rp: usize,
    stats: StatsChannel,
    seek_request: u64,
    seek_done: u64,
    read_done: u64,
    bytes_read: u64,
}

impl<F> BackReadBuf<F>
where
    F: BorrowMut<File>,
{
    pub async fn new(file: F, pos: u64, stats: StatsChannel) -> Result<Self, Error> {
        let mut ret = Self {
            file,
            buf: vec![0; 1024 * 8],
            abs: pos,
            wp: 0,
            rp: 0,
            stats,
            seek_request: 0,
            seek_done: 0,
            read_done: 0,
            bytes_read: 0,
        };
        ret.seek(pos).await?;
        Ok(ret)
    }

    pub fn into_file(self) -> F {
        //self.file
        err::todoval()
    }

    pub fn len(&self) -> usize {
        self.wp - self.rp
    }

    pub fn adv(&mut self, n: usize) {
        self.rp += n;
    }

    pub fn data(&self) -> &[u8] {
        &self.buf[self.rp..self.wp]
    }

    async fn fill(&mut self) -> Result<usize, Error> {
        if self.rp != 0 && self.rp == self.wp {
            self.wp = 0;
            self.rp = 0;
        } else {
            unsafe {
                std::ptr::copy::<u8>(&self.buf[self.rp], &mut self.buf[0], self.len());
                self.wp -= self.rp;
                self.rp = 0;
            }
        }
        let n = read(&mut self.file.borrow_mut(), &mut self.buf[self.wp..], &self.stats).await?;
        //debug!("I/O fill  n {}", n);
        self.wp += n;
        self.read_done += 1;
        self.bytes_read += n as u64;
        Ok(n)
    }

    pub async fn fill_min(&mut self, min: usize) -> Result<usize, Error> {
        let len = self.len();
        while self.len() < min {
            let n = self.fill().await?;
            if n == 0 {
                return Err(Error::with_msg_no_trace(format!("fill_min can not read min {}", min)));
            }
        }
        Ok(self.len() - len)
    }

    pub async fn seek(&mut self, pos: u64) -> Result<u64, Error> {
        if pos >= self.abs && pos < self.abs + self.buf.len() as u64 - 64 {
            self.rp = (pos - self.abs) as usize;
            self.seek_request += 1;
            Ok(pos)
        } else {
            //debug!("I/O seek  dp {}", dp);
            let s0 = pos.min(1024 * 2 - 256);
            self.abs = pos - s0;
            self.rp = 0;
            self.wp = 0;
            let ret = seek(self.file.borrow_mut(), SeekFrom::Start(self.abs), &self.stats)
                .await
                .map_err(|e| Error::from(e))?;
            self.fill_min(s0 as usize).await?;
            self.rp = s0 as usize;
            self.seek_request += 1;
            self.seek_done += 1;
            Ok(ret)
        }
    }

    pub fn rp_abs(&self) -> u64 {
        self.abs as u64 + self.rp as u64
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl<F> fmt::Debug for BackReadBuf<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BackReadBuf")
            .field("abs", &self.abs)
            .field("wp", &self.wp)
            .field("rp", &self.rp)
            .field("seek_request", &self.seek_request)
            .field("seek_done", &self.seek_done)
            .field("read_done", &self.read_done)
            .field("bytes_read", &self.bytes_read)
            .finish()
    }
}

impl<F> Drop for BackReadBuf<F> {
    fn drop(&mut self) {
        trace!("Drop {:?}", self);
    }
}
