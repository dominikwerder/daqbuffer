use crate::archeng::{
    read_exact, read_string, readf64, readu16, readu32, seek, RingBuf, StatsChannel, EPICS_EPOCH_OFFSET,
};
use crate::eventsitem::EventsItem;
use crate::plainevents::{PlainEvents, ScalarPlainEvents};
use err::Error;
use items::eventvalues::EventValues;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{NanoRange, Nanos};
use std::convert::TryInto;
use std::io::SeekFrom;
use tokio::fs::File;

use super::indextree::DataheaderPos;

#[derive(Debug)]
enum DbrType {
    DbrString = 0,
    DbrInt = 1,
    DbrStsFloat = 9,
    DbrTimeDouble = 20,
}

impl DbrType {
    fn from_u16(k: u16) -> Result<Self, Error> {
        use DbrType::*;
        let res = match k {
            0 => DbrString,
            1 => DbrInt,
            9 => DbrStsFloat,
            20 => DbrTimeDouble,
            _ => {
                let msg = format!("not a valid/supported dbr type: {}", k);
                return Err(Error::with_msg_no_trace(msg));
            }
        };
        Ok(res)
    }

    #[allow(dead_code)]
    fn byte_len(&self) -> usize {
        use DbrType::*;
        match self {
            DbrString => 0,
            DbrInt => 4,
            DbrStsFloat => 1,
            DbrTimeDouble => 16,
        }
    }
}

#[derive(Debug)]
pub struct DatafileHeader {
    pos: DataheaderPos,
    dir_offset: u32,
    // Should be absolute file position of the next data header
    // together with `fname_next`.
    // But unfortunately not always set?
    next_offset: u32,
    prev_offset: u32,
    curr_offset: u32,
    pub num_samples: u32,
    ctrl_info_offset: u32,
    buf_size: u32,
    buf_free: u32,
    dbr_type: DbrType,
    dbr_count: usize,
    period: f64,
    ts_beg: Nanos,
    ts_end: Nanos,
    ts_next_file: Nanos,
    fname_next: String,
    fname_prev: String,
}

const DATA_HEADER_LEN_ON_DISK: usize = 72 + 40 + 40;

pub async fn read_datafile_header(
    file: &mut File,
    pos: DataheaderPos,
    stats: &StatsChannel,
) -> Result<DatafileHeader, Error> {
    seek(file, SeekFrom::Start(pos.0), stats).await?;
    let mut rb = RingBuf::new();
    rb.fill_min(file, DATA_HEADER_LEN_ON_DISK, stats).await?;
    let buf = rb.data();
    let dir_offset = readu32(buf, 0);
    let next_offset = readu32(buf, 4);
    let prev_offset = readu32(buf, 8);
    let curr_offset = readu32(buf, 12);
    let num_samples = readu32(buf, 16);
    let ctrl_info_offset = readu32(buf, 20);
    let buf_size = readu32(buf, 24);
    let buf_free = readu32(buf, 28);
    let dbr_type = DbrType::from_u16(readu16(buf, 32))?;
    let dbr_count = readu16(buf, 34);
    // 4 bytes padding.
    let period = readf64(buf, 40);
    let ts1a = readu32(buf, 48);
    let ts1b = readu32(buf, 52);
    let ts2a = readu32(buf, 56);
    let ts2b = readu32(buf, 60);
    let ts3a = readu32(buf, 64);
    let ts3b = readu32(buf, 68);
    let ts_beg = if ts1a != 0 || ts1b != 0 {
        ts1a as u64 * SEC + ts1b as u64 + EPICS_EPOCH_OFFSET
    } else {
        0
    };
    let ts_end = if ts3a != 0 || ts3b != 0 {
        ts3a as u64 * SEC + ts3b as u64 + EPICS_EPOCH_OFFSET
    } else {
        0
    };
    let ts_next_file = if ts2a != 0 || ts2b != 0 {
        ts2a as u64 * SEC + ts2b as u64 + EPICS_EPOCH_OFFSET
    } else {
        0
    };
    let fname_prev = read_string(&buf[72..112])?;
    let fname_next = read_string(&buf[112..152])?;
    let ret = DatafileHeader {
        pos,
        dir_offset,
        next_offset,
        prev_offset,
        curr_offset,
        num_samples,
        ctrl_info_offset,
        buf_size,
        buf_free,
        dbr_type,
        dbr_count: dbr_count as usize,
        period,
        ts_beg: Nanos { ns: ts_beg },
        ts_end: Nanos { ns: ts_end },
        ts_next_file: Nanos { ns: ts_next_file },
        fname_next,
        fname_prev,
    };
    Ok(ret)
}

pub async fn read_data_1(
    file: &mut File,
    datafile_header: &DatafileHeader,
    range: NanoRange,
    _expand: bool,
    stats: &StatsChannel,
) -> Result<EventsItem, Error> {
    // TODO handle expand mode
    let dhpos = datafile_header.pos.0 + DATA_HEADER_LEN_ON_DISK as u64;
    seek(file, SeekFrom::Start(dhpos), stats).await?;
    let res = match &datafile_header.dbr_type {
        DbrType::DbrTimeDouble => {
            if datafile_header.dbr_count == 1 {
                trace!("~~~~~~~~~~~~~~~~~~~~~   read  scalar  DbrTimeDouble");
                let mut evs = EventValues {
                    tss: vec![],
                    values: vec![],
                };
                let n1 = datafile_header.num_samples as usize;
                //let n2 = datafile_header.dbr_type.byte_len();
                let n2 = 2 + 2 + 4 + 4 + (4) + 8;
                let n3 = n1 * n2;
                let mut buf = vec![0; n3];
                read_exact(file, &mut buf, stats).await?;
                let mut p1 = 0;
                let mut ntot = 0;
                while p1 < n3 - n2 {
                    let _status = u16::from_be_bytes(buf[p1..p1 + 2].try_into().unwrap());
                    p1 += 2;
                    let _severity = u16::from_be_bytes(buf[p1..p1 + 2].try_into().unwrap());
                    p1 += 2;
                    let ts1a = u32::from_be_bytes(buf[p1..p1 + 4].try_into().unwrap());
                    p1 += 4;
                    let ts1b = u32::from_be_bytes(buf[p1..p1 + 4].try_into().unwrap());
                    p1 += 4;
                    let ts1 = ts1a as u64 * SEC + ts1b as u64 + EPICS_EPOCH_OFFSET;
                    p1 += 4;
                    let value = f64::from_be_bytes(buf[p1..p1 + 8].try_into().unwrap());
                    p1 += 8;
                    ntot += 1;
                    if ts1 >= range.beg && ts1 < range.end {
                        evs.tss.push(ts1);
                        evs.values.push(value);
                    }
                }
                info!("parsed block with {} / {} events", ntot, evs.tss.len());
                let evs = ScalarPlainEvents::Double(evs);
                let plain = PlainEvents::Scalar(evs);
                let item = EventsItem::Plain(plain);
                item
            } else {
                let msg = format!("dbr_count {:?} not yet supported", datafile_header.dbr_count);
                error!("{}", msg);
                return Err(Error::with_msg_no_trace(msg));
            }
        }
        _ => {
            let msg = format!("Type {:?} not yet supported", datafile_header.dbr_type);
            error!("{}", msg);
            return Err(Error::with_msg_no_trace(msg));
        }
    };
    Ok(res)
}
