use arrayref::array_ref;
use err::Error;
use netpod::log::*;
use netpod::{ChannelConfig, Nanos, Node};
use std::mem::size_of;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, ErrorKind, SeekFrom};

pub async fn find_start_pos_for_config(
    ts: Nanos,
    channel_config: &ChannelConfig,
    node: &Node,
) -> Result<Option<u64>, Error> {
    let index_path = super::paths::index_path(ts, channel_config, node)?;
    let ret = match OpenOptions::new().open(&index_path).await {
        Ok(_file) => {
            info!("opened index file");
            error!("???????????????    TODO   search index for start");
            err::todoval::<u32>();
            None
        }
        Err(e) => match e.kind() {
            ErrorKind::NotFound => None,
            _ => Err(e)?,
        },
    };
    Ok(ret)
}

pub fn find_ge(h: u64, buf: &[u8]) -> Result<Option<(u64, u64)>, Error> {
    const N: usize = 2 * size_of::<u64>();
    let n1 = buf.len();
    if n1 % N != 0 {
        return Err(Error::with_msg(format!("find_ge  bad len {}", n1)));
    }
    if n1 == 0 {
        warn!("Empty index data");
        return Ok(None);
    }
    let n1 = n1 / N;
    let a = unsafe {
        let ptr = &buf[0] as *const u8 as *const ([u8; 8], [u8; 8]);
        std::slice::from_raw_parts(ptr, n1)
    };
    let mut j = 0;
    let mut k = n1 - 1;
    let x = u64::from_be_bytes(a[j].0);
    let y = u64::from_be_bytes(a[k].0);
    if x >= h {
        return Ok(Some((u64::from_be_bytes(a[j].0), u64::from_be_bytes(a[j].1))));
    }
    if y < h {
        return Ok(None);
    }
    loop {
        if k - j < 2 {
            let ret = (u64::from_be_bytes(a[k].0), u64::from_be_bytes(a[k].1));
            return Ok(Some(ret));
        }
        let m = (k + j) / 2;
        let x = u64::from_be_bytes(a[m].0);
        if x < h {
            j = m;
        } else {
            k = m;
        }
    }
}

async fn read(buf: &mut [u8], file: &mut File) -> Result<usize, Error> {
    let mut wp = 0;
    loop {
        let n1 = file.read(&mut buf[wp..]).await?;
        if n1 == 0 {
            break;
        } else {
            wp += n1;
        }
        if wp >= buf.len() {
            break;
        }
    }
    Ok(wp)
}

pub fn parse_channel_header(buf: &[u8]) -> Result<(u32,), Error> {
    if buf.len() < 6 {
        return Err(Error::with_msg(format!("parse_channel_header  buf len: {}", buf.len())));
    }
    let ver = i16::from_be_bytes(*array_ref![buf, 0, 2]);
    if ver != 0 {
        return Err(Error::with_msg(format!("unknown file version: {}", ver)));
    }
    let len1 = u32::from_be_bytes(*array_ref![buf, 2, 4]);
    if len1 < 9 || len1 > 256 {
        return Err(Error::with_msg(format!("unexpected data file header len1: {}", len1)));
    }
    if buf.len() < 2 + len1 as usize {
        return Err(Error::with_msg(format!(
            "data file header not contained in buffer len1: {} vs {}",
            len1,
            buf.len()
        )));
    }
    let len2 = u32::from_be_bytes(*array_ref![buf, 2 + len1 as usize - 4, 4]);
    if len1 != len2 {
        return Err(Error::with_msg(format!("len mismatch  len1: {}  len2: {}", len1, len2)));
    }
    Ok((len1 as u32,))
}

pub fn parse_event(buf: &[u8]) -> Result<(u32, Nanos), Error> {
    if buf.len() < 4 {
        return Err(Error::with_msg(format!("parse_event  buf len: {}", buf.len())));
    }
    let len1 = u32::from_be_bytes(*array_ref![buf, 0, 4]);
    if len1 < 9 || len1 > 512 {
        return Err(Error::with_msg(format!("unexpected event len1: {}", len1)));
    }
    if buf.len() < len1 as usize {
        return Err(Error::with_msg(format!(
            "event not contained in buffer len1: {} vs {}",
            len1,
            buf.len()
        )));
    }
    let len2 = u32::from_be_bytes(*array_ref![buf, len1 as usize - 4, 4]);
    if len1 != len2 {
        return Err(Error::with_msg(format!("len mismatch  len1: {}  len2: {}", len1, len2)));
    }
    let ts = u64::from_be_bytes(*array_ref![buf, 12, 8]);
    Ok((len1 as u32, Nanos { ns: ts }))
}

pub async fn read_event_at(pos: u64, file: &mut File) -> Result<(u32, Nanos), Error> {
    file.seek(SeekFrom::Start(pos)).await?;
    let mut buf = vec![0; 1024];
    let _n1 = read(&mut buf, file).await?;
    let ev = parse_event(&buf)?;
    Ok(ev)
}

pub async fn position_file(mut file: File, beg: u64) -> Result<File, Error> {
    // Read first chunk which should include channel name packet, and a first event.
    // It can be that file is empty.
    // It can be that there is a a channel header but zero events.
    let flen = file.seek(SeekFrom::End(0)).await?;
    file.seek(SeekFrom::Start(0)).await?;
    let mut buf = vec![0; 1024];
    let n1 = read(&mut buf, &mut file).await?;
    if n1 < buf.len() {
        // file has less content than our buffer
    } else {
        //
    }
    let hres = parse_channel_header(&buf)?;
    let headoff = 2 + hres.0 as u64;
    let ev = parse_event(&buf[headoff as usize..])?;
    let evlen = ev.0 as u64;
    let mut j = headoff;
    let mut k = ((flen - headoff) / evlen - 1) * evlen + headoff;
    let x = ev.1.ns;
    let y = read_event_at(k, &mut file).await?.1.ns;
    if x >= beg {
        file.seek(SeekFrom::Start(j)).await?;
        return Ok(file);
    }
    if y < beg {
        file.seek(SeekFrom::Start(j)).await?;
        return Ok(file);
    }
    loop {
        if k - j < 2 * evlen {
            file.seek(SeekFrom::Start(k)).await?;
            return Ok(file);
        }
        let m = j + (k - j) / 2 / evlen * evlen;
        let x = read_event_at(m, &mut file).await?.1.ns;
        if x < beg {
            j = m;
        } else {
            k = m;
        }
    }
}
