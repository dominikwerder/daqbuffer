use super::paths;
use bytes::BytesMut;
use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::timeunits::MS;
use netpod::{ChannelConfig, NanoRange, Nanos, Node};
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
    let tgt_tb = (range.beg / MS) as f32 / (channel_config.time_bin_size.ns / MS) as f32;
    trace!("tgt_tb:  {:?}", tgt_tb);
    trace!("timebins found:  {:?}", timebins);
    for &tb in &timebins {
        let ts_bin = Nanos {
            ns: tb * channel_config.time_bin_size.ns,
        };
        if ts_bin.ns >= range.end {
            continue;
        }
        if ts_bin.ns + channel_config.time_bin_size.ns <= range.beg {
            continue;
        }
        debug!("opening tb {:?}", &tb);
        let path = paths::datapath(tb, &channel_config, &node);
        debug!("opening path {:?}", &path);
        let mut file = OpenOptions::new().read(true).open(&path).await?;
        debug!("opened file {:?}  {:?}", &path, &file);
        {
            let index_path = paths::index_path(ts_bin, &channel_config, &node)?;
            match OpenOptions::new().read(true).open(&index_path).await {
                Ok(mut index_file) => {
                    let meta = index_file.metadata().await?;
                    if meta.len() > 1024 * 1024 * 20 {
                        return Err(Error::with_msg(format!(
                            "too large index file  {} bytes  for {}",
                            meta.len(),
                            channel_config.channel.name
                        )));
                    }
                    if meta.len() < 2 {
                        return Err(Error::with_msg(format!(
                            "bad meta len {}  for {}",
                            meta.len(),
                            channel_config.channel.name
                        )));
                    }
                    if meta.len() % 16 != 2 {
                        return Err(Error::with_msg(format!(
                            "bad meta len {}  for {}",
                            meta.len(),
                            channel_config.channel.name
                        )));
                    }
                    let mut buf = BytesMut::with_capacity(meta.len() as usize);
                    buf.resize(buf.capacity(), 0);
                    debug!("read exact index file  {}  {}", buf.len(), buf.len() % 16);
                    index_file.read_exact(&mut buf).await?;
                    match super::index::find_ge(range.beg, &buf[2..])? {
                        Some(o) => {
                            debug!("FOUND ts IN INDEX: {:?}", o);
                            file.seek(SeekFrom::Start(o.1)).await?;
                        }
                        None => {
                            debug!("NOT FOUND IN INDEX");
                            file.seek(SeekFrom::End(0)).await?;
                        }
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => {
                        file = super::index::position_file(file, range.beg).await?;
                    }
                    _ => Err(e)?,
                },
            }
        }
        chtx.send(Ok(file)).await?;
    }
    // TODO keep track of number of running
    debug!("open_files_inner done");
    Ok(())
}
