use super::paths;
use bytes::BytesMut;
use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::{ChannelConfig, NanoRange, Nanos, Node};
use std::path::PathBuf;
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, ErrorKind, SeekFrom};

pub struct OpenedFile {
    pub path: PathBuf,
    pub file: Option<File>,
    pub positioned: bool,
    pub index: bool,
    pub nreads: u32,
}

pub fn open_files(
    range: &NanoRange,
    channel_config: &ChannelConfig,
    node: Node,
) -> async_channel::Receiver<Result<OpenedFile, Error>> {
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
    chtx: &async_channel::Sender<Result<OpenedFile, Error>>,
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
        let path = paths::datapath(tb, &channel_config, &node);
        let mut file = OpenOptions::new().read(true).open(&path).await?;
        let ret = {
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
                    index_file.read_exact(&mut buf).await?;
                    match super::index::find_ge(range.beg, &buf[2..])? {
                        Some(o) => {
                            file.seek(SeekFrom::Start(o.1)).await?;
                            OpenedFile {
                                file: Some(file),
                                path,
                                positioned: true,
                                index: true,
                                nreads: 0,
                            }
                        }
                        None => OpenedFile {
                            file: None,
                            path,
                            positioned: false,
                            index: true,
                            nreads: 0,
                        },
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => {
                        let ts1 = Instant::now();
                        let res = super::index::position_static_len_datafile(file, range.beg).await?;
                        let ts2 = Instant::now();
                        if false {
                            // TODO collect for stats:
                            let dur = ts2.duration_since(ts1);
                            info!("position_static_len_datafile took  ms {}", dur.as_millis());
                        }
                        file = res.0;
                        if res.1 {
                            OpenedFile {
                                file: Some(file),
                                path,
                                positioned: true,
                                index: false,
                                nreads: res.2,
                            }
                        } else {
                            OpenedFile {
                                file: None,
                                path,
                                positioned: false,
                                index: false,
                                nreads: 0,
                            }
                        }
                    }
                    _ => Err(e)?,
                },
            }
        };
        chtx.send(Ok(ret)).await?;
    }
    // TODO keep track of number of running
    Ok(())
}
