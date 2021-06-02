use async_channel::{bounded, Receiver};
use chrono::{DateTime, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::FutureExt;
use netpod::log::*;
use netpod::NodeConfigCached;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::ErrorKind;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use tokio_postgres::Client;

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeDiskIdent {
    pub rowid: i64,
    pub facility: i64,
    pub split: i32,
    pub hostname: String,
}

impl NodeDiskIdent {
    pub fn rowid(&self) -> i64 {
        self.rowid
    }
    pub fn facility(&self) -> i64 {
        self.facility
    }
}

fn _get_hostname() -> Result<String, Error> {
    let out = std::process::Command::new("hostname").output()?;
    Ok(String::from_utf8(out.stdout[..out.stdout.len() - 1].to_vec())?)
}

pub async fn get_node_disk_ident(node_config: &NodeConfigCached, dbc: &Client) -> Result<NodeDiskIdent, Error> {
    let rows = dbc.query("select nodes.rowid, facility, split, hostname from nodes, facilities where facilities.name = $1 and facility = facilities.rowid and hostname = $2", &[&node_config.node.backend, &node_config.node.host]).await?;
    if rows.len() != 1 {
        return Err(Error::with_msg(format!(
            "get_node can't find unique entry for {} {}",
            node_config.node.host, node_config.node.backend
        )));
    }
    let row = &rows[0];
    Ok(NodeDiskIdent {
        rowid: row.get(0),
        facility: row.get(1),
        split: row.get(2),
        hostname: row.get(3),
    })
}

async fn find_channel_names_from_config<F, Fut>(base_dir: impl AsRef<Path>, mut cb: F) -> Result<(), Error>
where
    F: FnMut(&str) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let mut path2: PathBuf = base_dir.as_ref().into();
    path2.push("config");
    let mut rd = tokio::fs::read_dir(&path2).await?;
    while let Ok(Some(entry)) = rd.next_entry().await {
        let fname = String::from_utf8(entry.file_name().into_vec())?;
        cb(&fname).await?;
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatedDbWithChannelNames {
    msg: String,
    count: u32,
}

pub struct UpdatedDbWithChannelNamesStream {
    errored: bool,
    data_complete: bool,
    #[allow(dead_code)]
    node_config: Pin<Box<NodeConfigCached>>,
    node_config_ref: &'static NodeConfigCached,
    client_fut: Option<Pin<Box<dyn Future<Output = Result<Client, Error>> + Send>>>,
    client: Option<Pin<Box<Client>>>,
    client_ref: Option<&'static Client>,
    ident_fut: Option<Pin<Box<dyn Future<Output = Result<NodeDiskIdent, Error>> + Send>>>,
    ident: Option<NodeDiskIdent>,
}

unsafe impl Send for UpdatedDbWithChannelNamesStream {}

impl UpdatedDbWithChannelNamesStream {
    pub fn new(node_config: NodeConfigCached) -> Result<Self, Error> {
        let node_config = Box::pin(node_config.clone());
        let node_config_ref = unsafe { &*(&node_config as &NodeConfigCached as *const _) };
        let mut ret = Self {
            errored: false,
            data_complete: false,
            node_config,
            node_config_ref,
            client_fut: None,
            client: None,
            client_ref: None,
            ident_fut: None,
            ident: None,
        };
        ret.client_fut = Some(Box::pin(crate::create_connection(ret.node_config_ref)));
        Ok(ret)
    }
}

impl Stream for UpdatedDbWithChannelNamesStream {
    type Item = Result<UpdatedDbWithChannelNames, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.errored {
                Ready(None)
            } else if self.data_complete {
                Ready(None)
            } else if let Some(fut) = &mut self.ident_fut {
                match fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        self.ident = Some(item);
                        self.ident_fut = None;
                        let ret = UpdatedDbWithChannelNames {
                            msg: format!("ALL DONE"),
                            count: 42,
                        };
                        self.data_complete = true;
                        Ready(Some(Ok(ret)))
                    }
                    Ready(Err(e)) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = &mut self.client_fut {
                match fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        self.client_fut = None;
                        self.client = Some(Box::pin(item));
                        self.client_ref = Some(unsafe { &*(&self.client.as_ref().unwrap() as &Client as *const _) });
                        self.ident_fut = Some(Box::pin(get_node_disk_ident(
                            self.node_config_ref,
                            self.client_ref.as_ref().unwrap(),
                        )));
                        let ret = UpdatedDbWithChannelNames {
                            msg: format!("Client opened connection"),
                            count: 42,
                        };
                        Ready(Some(Ok(ret)))
                    }
                    Ready(Err(e)) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else {
                Ready(None)
            };
        }
    }
}

pub async fn update_db_with_channel_names(
    node_config: &NodeConfigCached,
) -> Result<Receiver<Result<UpdatedDbWithChannelNames, Error>>, Error> {
    let dbc = crate::create_connection(node_config).await?;
    let node_disk_ident = get_node_disk_ident(node_config, &dbc).await?;
    let c1 = Arc::new(RwLock::new(0u32));
    dbc.query("begin", &[]).await?;
    let dbc = Arc::new(dbc);
    find_channel_names_from_config(&node_config.node.data_base_path, |ch| {
        let ch = ch.to_owned();
        let dbc = dbc.clone();
        let c1 = c1.clone();
        let fac = node_disk_ident.facility;
        async move {
            crate::delay_io_short().await;
            dbc.query(
                "insert into channels (facility, name) values ($1, $2) on conflict do nothing",
                &[&fac, &ch],
            )
            .await?;
            let c2 = {
                let mut g = c1.write()?;
                *g += 1;
                *g
            };
            if c2 % 200 == 0 {
                trace!("channels {:6}  current {}", c2, ch);
                dbc.query("commit", &[]).await?;
                crate::delay_io_medium().await;
                dbc.query("begin", &[]).await?;
            }
            Ok(())
        }
    })
    .await?;
    dbc.query("commit", &[]).await?;
    let _ret = UpdatedDbWithChannelNames {
        msg: format!("done"),
        count: *c1.read()?,
    };
    Ok(bounded(16).1)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatedDbWithAllChannelConfigs {
    count: u32,
}

pub async fn update_db_with_all_channel_configs(
    node_config: &NodeConfigCached,
) -> Result<UpdatedDbWithAllChannelConfigs, Error> {
    let dbc = crate::create_connection(node_config).await?;
    let dbc = Arc::new(dbc);
    let node_disk_ident = &get_node_disk_ident(node_config, &dbc).await?;
    let rows = dbc
        .query(
            "select rowid, facility, name from channels where facility = $1 order by facility, name",
            &[&node_config.node.backend],
        )
        .await?;
    let mut c1 = 0;
    dbc.query("begin", &[]).await?;
    let mut count_inserted = 0;
    let mut count_updated = 0;
    for row in rows {
        let rowid: i64 = row.try_get(0)?;
        let _facility: i64 = row.try_get(1)?;
        let channel: String = row.try_get(2)?;
        match update_db_with_channel_config(
            node_config,
            node_disk_ident,
            rowid,
            &channel,
            dbc.clone(),
            &mut count_inserted,
            &mut count_updated,
        )
        .await
        {
            /*Err(Error::ChannelConfigdirNotFound { .. }) => {
                warn!("can not find channel config {}", channel);
                crate::delay_io_medium().await;
            }*/
            Err(e) => {
                error!("{:?}", e);
                crate::delay_io_medium().await;
            }
            _ => {
                c1 += 1;
                if c1 % 200 == 0 {
                    trace!(
                        "channel no {:6}  inserted {:6}  updated {:6}",
                        c1,
                        count_inserted,
                        count_updated
                    );
                    dbc.query("commit", &[]).await?;
                    dbc.query("begin", &[]).await?;
                }
                crate::delay_io_short().await;
            }
        }
    }
    dbc.query("commit", &[]).await?;
    let ret = UpdatedDbWithAllChannelConfigs { count: c1 };
    Ok(ret)
}

pub async fn update_search_cache(node_config: &NodeConfigCached) -> Result<(), Error> {
    let dbc = crate::create_connection(node_config).await?;
    dbc.query("select update_cache()", &[]).await?;
    Ok(())
}

/**
Parse the config of the given channel and update database.
*/
pub async fn update_db_with_channel_config(
    node_config: &NodeConfigCached,
    node_disk_ident: &NodeDiskIdent,
    channel_id: i64,
    channel: &str,
    dbc: Arc<Client>,
    count_inserted: &mut usize,
    count_updated: &mut usize,
) -> Result<(), Error> {
    let path = node_config
        .node
        .data_base_path
        .join("config")
        .join(channel)
        .join("latest")
        .join("00000_Config");
    let meta = tokio::fs::metadata(&path).await?;
    if meta.len() > 8 * 1024 * 1024 {
        return Err(Error::with_msg("meta data too long"));
    }
    let rows = dbc
        .query(
            "select rowid, fileSize, parsedUntil, channel from configs where node = $1 and channel = $2",
            &[&node_disk_ident.rowid(), &channel_id],
        )
        .await?;
    if rows.len() > 1 {
        return Err(Error::with_msg("more than one row"));
    }
    let (config_id, do_parse) = if rows.len() > 0 {
        let row = &rows[0];
        let rowid: i64 = row.get(0);
        let file_size: u32 = row.get::<_, i64>(1) as u32;
        let parsed_until: u32 = row.get::<_, i64>(2) as u32;
        let _channel_id = row.get::<_, i64>(2) as i64;
        if meta.len() < file_size as u64 || meta.len() < parsed_until as u64 {
            dbc.query(
                "insert into configs_history (rowid_original, node, channel, fileSize, parsedUntil, config, tsinsert) select rowid as rowid_original, node, channel, fileSize, parsedUntil, config, now() from configs where rowid = $1",
                &[&rowid],
            ).await?;
        }
        //ensure!(meta.len() >= parsed_until as u64, ConfigFileOnDiskShrunk{path});
        (Some(rowid), true)
    } else {
        (None, true)
    };
    if do_parse {
        let buf = tokio::fs::read(&path).await?;
        let config = parse::channelconfig::parse_config(&buf)?;
        match config_id {
            None => {
                dbc.query(
                    "insert into configs (node, channel, fileSize, parsedUntil, config) values ($1, $2, $3, $4, $5)",
                    &[
                        &node_disk_ident.rowid(),
                        &channel_id,
                        &(meta.len() as i64),
                        &(buf.len() as i64),
                        &serde_json::to_value(config)?,
                    ],
                )
                .await?;
                *count_inserted += 1;
            }
            Some(_config_id_2) => {
                dbc.query(
                    "insert into configs (node, channel, fileSize, parsedUntil, config) values ($1, $2, $3, $4, $5) on conflict (node, channel) do update set fileSize = $3, parsedUntil = $4, config = $5",
                    &[&node_disk_ident.rowid(), &channel_id, &(meta.len() as i64), &(buf.len() as i64), &serde_json::to_value(config)?],
                ).await?;
                *count_updated += 1;
            }
        }
    }
    Ok(())
}

pub async fn update_db_with_all_channel_datafiles(
    node_config: &NodeConfigCached,
    node_disk_ident: &NodeDiskIdent,
    ks_prefix: &str,
) -> Result<(), Error> {
    let dbc = Arc::new(crate::create_connection(node_config).await?);
    let rows = dbc
        .query(
            "select rowid, facility, name from channels where facility = $1 order by facility, name",
            &[&node_disk_ident.facility()],
        )
        .await?;
    let mut c1 = 0;
    dbc.query("begin", &[]).await?;
    for row in rows {
        let rowid: i64 = row.try_get(0)?;
        let _facility: i64 = row.try_get(1)?;
        let channel: String = row.try_get(2)?;
        update_db_with_channel_datafiles(node_config, node_disk_ident, ks_prefix, rowid, &channel, dbc.clone()).await?;
        c1 += 1;
        if c1 % 40 == 0 {
            trace!("import datafiles  {}  {}", c1, channel);
            dbc.query("commit", &[]).await?;
            dbc.query("begin", &[]).await?;
        }
        if false && c1 >= 30 {
            break;
        }
    }
    dbc.query("commit", &[]).await?;
    Ok(())
}

struct DatafileDbWriter {
    channel_id: i64,
    node_id: i64,
    dbc: Arc<Client>,
    c1: Arc<RwLock<u32>>,
}

#[derive(Debug, Serialize)]
pub struct ChannelDesc {
    name: String,
}

#[derive(Debug, Serialize)]
pub struct ChannelDatafileDesc {
    channel: ChannelDesc,
    ks: u32,
    tb: u32,
    sp: u32,
    bs: u32,
    fs: u64,
    mt: DateTime<Utc>,
    ix_fs: Option<u64>,
    ix_mt: Option<DateTime<Utc>>,
}

impl ChannelDatafileDesc {
    pub fn timebin(&self) -> u32 {
        self.tb
    }
    pub fn binsize(&self) -> u32 {
        self.bs
    }
    pub fn keyspace(&self) -> u32 {
        self.ks
    }
    pub fn split(&self) -> u32 {
        self.sp
    }
}

pub trait ChannelDatafileDescSink {
    fn sink(&self, k: ChannelDatafileDesc) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
}

impl ChannelDatafileDescSink for DatafileDbWriter {
    fn sink(&self, k: ChannelDatafileDesc) -> Pin<Box<dyn Future<Output = Result<(), Error>>>> {
        let dbc = self.dbc.clone();
        let c1 = self.c1.clone();
        let channel_id = self.channel_id;
        let node_id = self.node_id;
        Box::pin(async move {
            dbc.query(
                "insert into datafiles (node, channel, tsbeg, tsend, props) values ($1, $2, $3, $4, $5) on conflict do nothing",
                &[
                    &node_id,
                    &channel_id,
                    &(k.timebin() as i64 * k.binsize() as i64),
                    &((k.timebin() + 1) as i64 * k.binsize() as i64),
                    &serde_json::to_value(k)?,
                ]
            ).await?;
            *c1.write()? += 1;
            Ok(())
        })
    }
}

pub async fn find_channel_datafiles_in_ks(
    base_dir: impl AsRef<Path>,
    ks_prefix: &str,
    ks: u32,
    channel: &str,
    cb: &dyn ChannelDatafileDescSink,
) -> Result<Option<()>, Error> {
    let data_dir_path: PathBuf = base_dir
        .as_ref()
        .join(format!("{}_{}", ks_prefix, ks))
        .join("byTime")
        .join(channel);
    let re1 = regex::Regex::new(r"^\d{19}$")?;
    let re2 = regex::Regex::new(r"^\d{10}$")?;
    let re4 = regex::Regex::new(r"^(\d{19})_0{5}_Data$")?;
    let mut rd = match tokio::fs::read_dir(&data_dir_path).await {
        Ok(k) => k,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => return Ok(None),
            _ => Err(e)?,
        },
    };
    while let Ok(Some(entry)) = rd.next_entry().await {
        let fname = String::from_utf8(entry.file_name().into_vec())?;
        if !re1.is_match(&fname) {
            warn!("unexpected file  {}", fname);
            continue;
        }
        let timebin: u32 = fname.parse()?;
        let path = data_dir_path.join(fname);
        let mut rd = tokio::fs::read_dir(&path).await?;
        while let Ok(Some(entry)) = rd.next_entry().await {
            let fname = String::from_utf8(entry.file_name().into_vec())?;
            if !re2.is_match(&fname) {
                warn!("unexpected file  {}", fname);
                continue;
            }
            let split: u32 = fname.parse()?;
            let path = path.join(fname);
            let mut rd = tokio::fs::read_dir(&path).await?;
            while let Ok(Some(entry)) = rd.next_entry().await {
                let fname = String::from_utf8(entry.file_name().into_vec())?;
                if let Some(m) = re4.captures(&fname) {
                    let binsize: u32 = m.get(1).unwrap().as_str().parse()?;
                    let data_path = path.join(&fname);
                    let meta = tokio::fs::metadata(&data_path).await?;
                    let index_path = path.join(format!("{}_Index", fname));
                    let (ix_size, ix_tmod) = if let Ok(meta) = tokio::fs::metadata(&index_path).await {
                        (Some(meta.len()), Some(meta.modified().unwrap().into()))
                    } else {
                        (None, None)
                    };
                    cb.sink(ChannelDatafileDesc {
                        channel: ChannelDesc { name: channel.into() },
                        ks: ks,
                        tb: timebin,
                        sp: split,
                        bs: binsize,
                        fs: meta.len(),
                        ix_fs: ix_size,
                        ix_mt: ix_tmod,
                        mt: meta.modified().unwrap().into(),
                    })
                    .await?;
                }
            }
        }
    }
    Ok(Some(()))
}

pub async fn update_db_with_channel_datafiles(
    node_config: &NodeConfigCached,
    node_disk_ident: &NodeDiskIdent,
    ks_prefix: &str,
    channel_id: i64,
    channel: &str,
    dbc: Arc<Client>,
) -> Result<(), Error> {
    let writer = DatafileDbWriter {
        node_id: node_disk_ident.rowid(),
        channel_id: channel_id,
        dbc: dbc.clone(),
        c1: Arc::new(RwLock::new(0)),
    };
    let mut n_nothing = 0;
    for ks in &[2, 3, 4] {
        match find_channel_datafiles_in_ks(&node_config.node.data_base_path, ks_prefix, *ks, channel, &writer).await {
            /*Err(Error::ChannelDatadirNotFound { .. }) => {
                n_nothing += 1;
            }*/
            Ok(None) => {
                n_nothing += 1;
            }
            x => {
                x?;
            }
        };
        if false && *writer.c1.read()? >= 10 {
            break;
        }
    }
    if n_nothing >= 3 {
        //warn!("No datafile directories in any keyspace  writer got {:5}  n_nothing {}  channel {}", writer.c1.borrow(), n_nothing, channel);
    }
    Ok(())
}
