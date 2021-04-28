use super::paths;
use bytes::BytesMut;
use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{ChannelConfig, NanoRange, Nanos, Node};
use std::mem::size_of;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, ErrorKind, SeekFrom};

pub fn open_files(
    range: &NanoRange,
    channel_config: &ChannelConfig,
    node: Node,
) -> async_channel::Receiver<Result<File, Error>> {
    let (chtx, chrx) = async_channel::bounded(2);
    let range = range.clone();
    let channel_config = channel_config.clone();
    tokio::spawn(async move {
        match open_files_inner(&chtx, &range, &channel_config, node).await {
            Ok(_) => {}
            Err(e) => match chtx.send(Err(e.into())).await {
                Ok(_) => {}
                Err(e) => {
                    error!("open_files  channel send error {:?}", e);
                }
            },
        }
    });
    chrx
}

async fn open_files_inner(
    chtx: &async_channel::Sender<Result<File, Error>>,
    range: &NanoRange,
    channel_config: &ChannelConfig,
    node: Node,
) -> Result<(), Error> {
    let channel_config = channel_config.clone();
    // TODO  reduce usage of `query` and see what we actually need.
    let mut timebins = vec![];
    {
        let rd = tokio::fs::read_dir(paths::channel_timebins_dir_path(&channel_config, &node)?).await?;
        let mut rd = tokio_stream::wrappers::ReadDirStream::new(rd);
        while let Some(e) = rd.next().await {
            let e = e?;
            let dn = e
                .file_name()
                .into_string()
                .map_err(|e| Error::with_msg(format!("Bad OS path {:?}", e)))?;
            let vv = dn.chars().fold(0, |a, x| if x.is_digit(10) { a + 1 } else { a });
            if vv == 19 {
                timebins.push(dn.parse::<u64>()?);
            }
        }
    }
    timebins.sort_unstable();
    info!("TIMEBINS FOUND:  {:?}", timebins);
    for &tb in &timebins {
        let ts_bin = Nanos {
            ns: tb * channel_config.time_bin_size,
        };
        if ts_bin.ns >= range.end {
            continue;
        }
        if ts_bin.ns + channel_config.time_bin_size <= range.beg {
            continue;
        }

        let path = paths::datapath(tb, &channel_config, &node);
        let mut file = OpenOptions::new().read(true).open(&path).await?;
        info!("opened file {:?}  {:?}", &path, &file);

        {
            let index_path = paths::index_path(ts_bin, &channel_config, &node)?;
            match OpenOptions::new().read(true).open(&index_path).await {
                Ok(mut index_file) => {
                    let meta = index_file.metadata().await?;
                    if meta.len() > 1024 * 1024 * 10 {
                        return Err(Error::with_msg(format!(
                            "too large index file  {} bytes  for {}",
                            meta.len(),
                            channel_config.channel.name
                        )));
                    }
                    if meta.len() % 16 != 0 {
                        return Err(Error::with_msg(format!(
                            "bad meta len {}  for {}",
                            meta.len(),
                            channel_config.channel.name
                        )));
                    }
                    let mut buf = BytesMut::with_capacity(meta.len() as usize);
                    buf.resize(buf.capacity(), 0);
                    info!("read exact index file  {}  {}", buf.len(), buf.len() % 16);
                    index_file.read_exact(&mut buf).await?;
                    match find_ge(range.beg, &buf)? {
                        Some(o) => {
                            info!("FOUND ts IN INDEX: {:?}", o);
                            file.seek(SeekFrom::Start(o.1)).await?;
                        }
                        None => {
                            info!("NOT FOUND IN INDEX");
                            file.seek(SeekFrom::End(0)).await?;
                        }
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => {
                        // TODO Read first 1k, assume that channel header fits.
                        // TODO Seek via binary search. Can not read whole file into memory!
                        todo!("Seek directly in scalar file");
                    }
                    _ => Err(e)?,
                },
            }
        }

        // TODO Since I want to seek into the data file, the consumer of this channel must not expect a file channel name header.

        chtx.send(Ok(file)).await?;
    }
    Ok(())
}

fn find_ge(h: u64, buf: &[u8]) -> Result<Option<(u64, u64)>, Error> {
    trace!("find_ge {}", h);
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
    trace!("first/last ts:  {}  {}", x, y);
    if x >= h {
        return Ok(Some((u64::from_be_bytes(a[j].0), u64::from_be_bytes(a[j].1))));
    }
    if y < h {
        return Ok(None);
    }
    loop {
        if k - j < 2 {
            let ret = (u64::from_be_bytes(a[k].0), u64::from_be_bytes(a[k].1));
            trace!("FOUND  {:?}", ret);
            return Ok(Some(ret));
        }
        let m = (k + j) / 2;
        let x = u64::from_be_bytes(a[m].0);
        trace!("CHECK NEW M: {}", x);
        if x < h {
            j = m;
        } else {
            k = m;
        }
    }
}
