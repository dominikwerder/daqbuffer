use crate::archeng::indextree::DataheaderPos;
use crate::archeng::{format_hex_block, read_string, readf64, readu16, readu32, StatsChannel, EPICS_EPOCH_OFFSET};
use commonio::ringbuf::RingBuf;
use commonio::{read_exact, seek};
use err::Error;
use items::eventsitem::EventsItem;
use items::plainevents::{PlainEvents, ScalarPlainEvents, WavePlainEvents};
use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use netpod::log::*;
use netpod::timeunits::SEC;
use netpod::{NanoRange, Nanos};
use std::convert::TryInto;
use std::io::SeekFrom;
use tokio::fs::File;

#[derive(Clone, Debug)]
enum DbrType {
    DbrString = 0,
    DbrShort = 1,
    DbrStsFloat = 9,
    DbrTimeString = 14,
    DbrTimeShort = 15,
    DbrTimeFloat = 16,
    DbrTimeEnum = 17,
    DbrTimeChar = 18,
    DbrTimeLong = 19,
    DbrTimeDouble = 20,
}

impl DbrType {
    fn from_u16(k: u16) -> Result<Self, Error> {
        use DbrType::*;
        let res = match k {
            0 => DbrString,
            1 => DbrShort,
            9 => DbrStsFloat,
            14 => DbrTimeString,
            15 => DbrTimeShort,
            16 => DbrTimeFloat,
            17 => DbrTimeEnum,
            18 => DbrTimeChar,
            19 => DbrTimeLong,
            20 => DbrTimeDouble,
            _ => {
                let msg = format!("not a valid/supported dbr type: {}", k);
                return Err(Error::with_msg_no_trace(msg));
            }
        };
        Ok(res)
    }

    fn meta_len(&self) -> usize {
        use DbrType::*;
        match self {
            DbrString => 0,
            DbrShort => 0,
            DbrStsFloat => 4,
            DbrTimeString => 12,
            DbrTimeShort => 12,
            DbrTimeFloat => 12,
            DbrTimeEnum => 12,
            DbrTimeChar => 12,
            DbrTimeLong => 12,
            DbrTimeDouble => 12,
        }
    }

    fn pad_meta(&self) -> usize {
        use DbrType::*;
        match self {
            DbrString => 0,
            DbrShort => 0,
            DbrStsFloat => 0,
            DbrTimeString => 0,
            DbrTimeShort => 2,
            DbrTimeFloat => 0,
            DbrTimeEnum => 2,
            DbrTimeChar => 3,
            DbrTimeLong => 0,
            DbrTimeDouble => 4,
        }
    }

    fn val_len(&self) -> usize {
        use DbrType::*;
        match self {
            DbrString => 40,
            DbrShort => 2,
            DbrStsFloat => 4,
            DbrTimeString => 40,
            DbrTimeShort => 2,
            DbrTimeFloat => 4,
            DbrTimeEnum => 2,
            DbrTimeChar => 1,
            DbrTimeLong => 4,
            DbrTimeDouble => 8,
        }
    }

    fn msg_len(&self, count: usize) -> usize {
        let n = self.meta_len() + self.pad_meta() + count * self.val_len();
        let r = n % 8;
        let n = if r == 0 { n } else { n + 8 - r };
        n
    }
}

#[derive(Debug)]
pub struct DatafileHeader {
    pos: DataheaderPos,
    #[allow(unused)]
    dir_offset: u32,
    // Should be absolute file position of the next data header
    // together with `fname_next`.
    // But unfortunately not always set?
    #[allow(unused)]
    next_offset: u32,
    #[allow(unused)]
    prev_offset: u32,
    #[allow(unused)]
    curr_offset: u32,
    pub num_samples: u32,
    #[allow(unused)]
    ctrl_info_offset: u32,
    buf_size: u32,
    #[allow(unused)]
    buf_free: u32,
    dbr_type: DbrType,
    dbr_count: usize,
    #[allow(unused)]
    period: f64,
    #[allow(unused)]
    ts_beg: Nanos,
    #[allow(unused)]
    ts_end: Nanos,
    #[allow(unused)]
    ts_next_file: Nanos,
    #[allow(unused)]
    fname_next: String,
    #[allow(unused)]
    fname_prev: String,
}

const DATA_HEADER_LEN_ON_DISK: usize = 72 + 40 + 40;

// TODO retire this version (better version reads from buffer)
pub async fn read_datafile_header(
    file: &mut File,
    pos: DataheaderPos,
    stats: &StatsChannel,
) -> Result<DatafileHeader, Error> {
    let mut rb = RingBuf::new(file, pos.0, stats.clone()).await?;
    rb.fill_min(DATA_HEADER_LEN_ON_DISK).await?;
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

pub async fn read_datafile_header2(rb: &mut RingBuf<File>, pos: DataheaderPos) -> Result<DatafileHeader, Error> {
    // TODO avoid the extra seek: make sure that RingBuf catches this. Profile..
    rb.seek(pos.0).await?;
    rb.fill_min(DATA_HEADER_LEN_ON_DISK).await?;
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
    rb.adv(DATA_HEADER_LEN_ON_DISK);
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

trait MetaParse {
    fn parse_meta(buf: &[u8]) -> (u64, usize);
}

struct NoneMetaParse;

impl MetaParse for NoneMetaParse {
    #[inline(always)]
    fn parse_meta(_buf: &[u8]) -> (u64, usize) {
        (0, 0)
    }
}

struct TimeMetaParse;

impl MetaParse for TimeMetaParse {
    #[inline(always)]
    fn parse_meta(buf: &[u8]) -> (u64, usize) {
        let tsa = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let tsb = u32::from_be_bytes(buf[8..12].try_into().unwrap());
        let ts = tsa as u64 * SEC + tsb as u64 + EPICS_EPOCH_OFFSET;
        (ts, 12)
    }
}

#[inline(always)]
fn parse_msg<MP: MetaParse, F, VT>(
    buf: &[u8],
    _meta_parse: MP,
    dbrt: DbrType,
    dbrcount: usize,
    valf: F,
) -> Result<(u64, VT, usize), Error>
where
    F: Fn(&[u8], usize) -> VT,
{
    let (ts, n) = MP::parse_meta(buf);
    let buf = &buf[n + dbrt.pad_meta()..];
    Ok((ts, valf(buf, dbrcount), n))
}

macro_rules! ex_s {
    ($sty:ident, $n:ident) => {
        fn $n(buf: &[u8], _dbrcount: usize) -> $sty {
            const R: usize = std::mem::size_of::<$sty>();
            $sty::from_be_bytes(buf[0..R].try_into().unwrap())
        }
    };
}

macro_rules! ex_v {
    ($sty:ident, $n:ident) => {
        fn $n(mut buf: &[u8], dbrcount: usize) -> Vec<$sty> {
            const R: usize = std::mem::size_of::<$sty>();
            let mut a = Vec::with_capacity(dbrcount);
            for _ in 0..dbrcount {
                let v = $sty::from_be_bytes(buf[0..R].try_into().unwrap());
                a.push(v);
                buf = &buf[R..];
            }
            a
        }
    };
}

ex_s!(i8, ex_s_i8);
ex_s!(i16, ex_s_i16);
ex_s!(i32, ex_s_i32);
ex_s!(f32, ex_s_f32);
ex_s!(f64, ex_s_f64);

ex_v!(i8, ex_v_i8);
ex_v!(i16, ex_v_i16);
ex_v!(i32, ex_v_i32);
ex_v!(f32, ex_v_f32);
ex_v!(f64, ex_v_f64);

macro_rules! read_msg {
    ($sty:ident, $exfs:ident, $exfv:ident, $evvar:ident, $rb:expr, $msglen:expr, $numsamples:expr, $dbrt:expr, $dbrcount:ident) => {
        if $dbrcount == 1 {
            let mut evs = ScalarEvents::empty();
            for _ in 0..$numsamples {
                $rb.fill_min($msglen).await?;
                let buf = $rb.data();
                let (ts, val, _) = parse_msg(buf, TimeMetaParse, $dbrt.clone(), $dbrcount, $exfs)?;
                evs.tss.push(ts);
                evs.values.push(val);
                $rb.adv($msglen);
            }
            let evs = ScalarPlainEvents::$evvar(evs);
            let plain = PlainEvents::Scalar(evs);
            let item = EventsItem::Plain(plain);
            item
        } else {
            let mut evs = WaveEvents::empty();
            for _ in 0..$numsamples {
                $rb.fill_min($msglen).await?;
                let buf = $rb.data();
                let (ts, val, _) = parse_msg(buf, TimeMetaParse, $dbrt.clone(), $dbrcount, $exfv)?;
                evs.tss.push(ts);
                evs.vals.push(val);
                $rb.adv($msglen);
            }
            let evs = WavePlainEvents::$evvar(evs);
            let plain = PlainEvents::Wave(evs);
            let item = EventsItem::Plain(plain);
            item
        }
    };
}

async fn _format_debug_1(rb: &mut RingBuf<File>, dbrcount: usize) -> Result<(), Error> {
    rb.fill_min(1024 * 10).await?;
    for i1 in 0..19 {
        let hex = format_hex_block(&rb.data()[512 * i1..], 512);
        error!("dbrcount {}   block\n{}", dbrcount, hex);
    }
    return Err(Error::with_msg_no_trace("EXIT"));
}

fn _format_debug_2(evs: WaveEvents<i32>) -> Result<(), Error> {
    info!("tss: {:?}", evs.tss);
    let n = evs.vals.len();
    let vals: Vec<_> = evs
        .vals
        .iter()
        .enumerate()
        .filter(|&(i, _)| i < 3 || i + 3 >= n)
        .map(|(_i, j)| {
            if j.len() > 6 {
                let mut a = j[0..3].to_vec();
                a.extend_from_slice(&j[j.len() - 3..]);
                a.to_vec()
            } else {
                j.to_vec()
            }
        })
        .collect();
    info!("vals: {:?}", vals);
    Ok(())
}

pub async fn read_data2(
    rb: &mut RingBuf<File>,
    datafile_header: &DatafileHeader,
    _range: NanoRange,
    _expand: bool,
) -> Result<EventsItem, Error> {
    // TODO handle range
    // TODO handle expand mode
    {
        let dpos = datafile_header.pos.0 + DATA_HEADER_LEN_ON_DISK as u64;
        if rb.rp_abs() != dpos {
            warn!("read_data2 rb not positioned  {} vs {}", rb.rp_abs(), dpos);
            rb.seek(dpos).await?;
        }
    }
    let numsamples = datafile_header.num_samples as usize;
    let dbrcount = datafile_header.dbr_count;
    let dbrt = datafile_header.dbr_type.clone();
    let dbrt = if let DbrType::DbrTimeEnum = dbrt {
        DbrType::DbrTimeShort
    } else {
        dbrt
    };
    let msg_len = dbrt.msg_len(dbrcount);
    {
        if (datafile_header.buf_size as usize) < numsamples * msg_len {
            return Err(Error::with_msg_no_trace(format!(
                "buffer too small for data  {}  {}  {}",
                datafile_header.buf_size, numsamples, msg_len
            )));
        }
    }
    if dbrcount == 0 {
        return Err(Error::with_msg_no_trace(format!("unexpected dbrcount {}", dbrcount)));
    }
    let res = match &dbrt {
        DbrType::DbrTimeChar => read_msg!(i8, ex_s_i8, ex_v_i8, I8, rb, msg_len, numsamples, dbrt, dbrcount),
        DbrType::DbrTimeShort => read_msg!(i16, ex_s_i16, ex_v_i16, I16, rb, msg_len, numsamples, dbrt, dbrcount),
        DbrType::DbrTimeLong => read_msg!(i32, ex_s_i32, ex_v_i32, I32, rb, msg_len, numsamples, dbrt, dbrcount),
        DbrType::DbrTimeFloat => read_msg!(f32, ex_s_f32, ex_v_f32, F32, rb, msg_len, numsamples, dbrt, dbrcount),
        DbrType::DbrTimeDouble => read_msg!(f64, ex_s_f64, ex_v_f64, F64, rb, msg_len, numsamples, dbrt, dbrcount),
        DbrType::DbrTimeString => {
            if dbrcount == 1 {
                // TODO
                let evs = ScalarPlainEvents::I8(ScalarEvents::empty());
                let plain = PlainEvents::Scalar(evs);
                let item = EventsItem::Plain(plain);
                item
            } else {
                // TODO
                let evs = WavePlainEvents::F64(WaveEvents::empty());
                let plain = PlainEvents::Wave(evs);
                let item = EventsItem::Plain(plain);
                item
            }
        }
        DbrType::DbrTimeEnum | DbrType::DbrShort | DbrType::DbrString | DbrType::DbrStsFloat => {
            let msg = format!("Type {:?} not yet supported", datafile_header.dbr_type);
            error!("{}", msg);
            return Err(Error::with_msg_no_trace(msg));
        }
    };
    Ok(res)
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
                let mut evs = ScalarEvents {
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
                debug!("parsed block with {} / {} events", ntot, evs.tss.len());
                let evs = ScalarPlainEvents::F64(evs);
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
