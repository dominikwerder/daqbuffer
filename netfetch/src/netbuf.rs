use err::Error;
use tokio::io::ReadBuf;

pub const BUFCAP: usize = 1024 * 128;
pub const RP_REW_PT: usize = 1024 * 64;

pub struct NetBuf {
    buf: Vec<u8>,
    wp: usize,
    rp: usize,
}

impl NetBuf {
    pub fn new() -> Self {
        Self {
            buf: vec![0; BUFCAP],
            wp: 0,
            rp: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.wp - self.rp
    }

    pub fn cap(&self) -> usize {
        self.buf.len()
    }

    pub fn wrcap(&self) -> usize {
        self.buf.len() - self.wp
    }

    pub fn data(&self) -> &[u8] {
        &self.buf[self.rp..self.wp]
    }

    pub fn adv(&mut self, x: usize) -> Result<(), Error> {
        if self.len() < x {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            self.rp += x;
            Ok(())
        }
    }

    pub fn wpadv(&mut self, x: usize) -> Result<(), Error> {
        if self.wrcap() < x {
            return Err(Error::with_msg_no_trace("not enough space"));
        } else {
            self.wp += x;
            Ok(())
        }
    }

    pub fn read_u8(&mut self) -> Result<u8, Error> {
        type T = u8;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = self.buf[self.rp];
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_u64(&mut self) -> Result<u64, Error> {
        type T = u64;
        const TS: usize = std::mem::size_of::<T>();
        if self.len() < TS {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = T::from_be_bytes(self.buf[self.rp..self.rp + TS].try_into()?);
            self.rp += TS;
            Ok(val)
        }
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], Error> {
        if self.len() < n {
            return Err(Error::with_msg_no_trace("not enough bytes"));
        } else {
            let val = self.buf[self.rp..self.rp + n].as_ref();
            self.rp += n;
            Ok(val)
        }
    }

    pub fn read_buf_for_fill(&mut self) -> ReadBuf {
        self.rewind_if_needed();
        let read_buf = ReadBuf::new(&mut self.buf[self.wp..]);
        read_buf
    }

    pub fn rewind_if_needed(&mut self) {
        if self.rp != 0 && self.rp == self.wp {
            self.rp = 0;
            self.wp = 0;
        } else if self.rp > RP_REW_PT {
            self.buf.copy_within(self.rp..self.wp, 0);
            self.wp -= self.rp;
            self.rp = 0;
        }
    }

    pub fn put_slice(&mut self, buf: &[u8]) -> Result<(), Error> {
        self.rewind_if_needed();
        if self.wrcap() < buf.len() {
            return Err(Error::with_msg_no_trace("not enough space"));
        } else {
            self.buf[self.wp..self.wp + buf.len()].copy_from_slice(buf);
            self.wp += buf.len();
            Ok(())
        }
    }
}
