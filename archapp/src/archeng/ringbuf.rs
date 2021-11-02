use crate::archeng::{read, seek, StatsChannel};
use err::Error;
use netpod::log::*;
use std::fmt;
use std::mem::ManuallyDrop;
use std::{borrow::BorrowMut, io::SeekFrom};
use tokio::fs::File;

pub struct RingBuf<F> {
    file: Option<F>,
    buf: Vec<u8>,
    abs: usize,
    wp: usize,
    rp: usize,
    stats: StatsChannel,
    seek_request: u64,
    seek_done: u64,
    read_done: u64,
}

impl<F> RingBuf<F>
where
    F: BorrowMut<File>,
{
    pub async fn new(file: F, pos: u64, stats: StatsChannel) -> Result<Self, Error> {
        let mut ret = Self {
            file: Some(file),
            buf: vec![0; 1024 * 1024],
            abs: usize::MAX,
            wp: 0,
            rp: 0,
            stats,
            seek_request: 0,
            seek_done: 0,
            read_done: 0,
        };
        ret.seek(pos).await?;
        Ok(ret)
    }

    pub fn into_file(mut self) -> F {
        self.file.take().unwrap()
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
        if self.rp == self.wp {
            if self.rp != 0 {
                self.wp = 0;
                self.rp = 0;
            }
        } else {
            unsafe {
                std::ptr::copy::<u8>(&self.buf[self.rp], &mut self.buf[0], self.len());
                self.wp -= self.rp;
                self.rp = 0;
            }
        }
        let n = read(
            self.file.as_mut().unwrap().borrow_mut(),
            &mut self.buf[self.wp..],
            &self.stats,
        )
        .await?;
        self.wp += n;
        self.read_done += 1;
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
        let dp = pos as i64 - self.abs as i64 - self.rp as i64;
        if dp < 0 && dp > -2048 {
            debug!("small NEG seek {}", dp);
        } else if dp == 0 {
            debug!("zero seek");
        } else if dp > 0 && dp < 2048 {
            debug!("small POS seek {}", dp);
        }
        self.abs = pos as usize;
        self.rp = 0;
        self.wp = 0;
        let ret = seek(
            self.file.as_mut().unwrap().borrow_mut(),
            SeekFrom::Start(pos),
            &self.stats,
        )
        .await
        .map_err(|e| Error::from(e))?;
        self.seek_request += 1;
        self.seek_done += 1;
        Ok(ret)
    }

    pub fn rp_abs(&self) -> u64 {
        self.abs as u64 + self.rp as u64
    }
}

impl<F> fmt::Debug for RingBuf<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RingBuf")
            .field("abs", &self.abs)
            .field("wp", &self.wp)
            .field("rp", &self.rp)
            .finish()
    }
}

impl<F> Drop for RingBuf<F> {
    fn drop(&mut self) {
        info!("RingBuf  Drop  {}  {}", self.seek_request, self.read_done);
    }
}
