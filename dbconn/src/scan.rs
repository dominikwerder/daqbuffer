#![allow(unused_imports)]
use async_channel::{bounded, Receiver};
use chrono::{DateTime, Utc};
use err::Error;
use futures_core::Stream;
use futures_util::{pin_mut, FutureExt, StreamExt};
use netpod::log::*;
use netpod::{Database, NodeConfigCached};
use parse::channelconfig::NErr;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::ErrorKind;
use std::marker::PhantomPinned;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use tokio::fs::{DirEntry, ReadDir};
use tokio_postgres::Client;

use crate::ErrConv;

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
    let sql = "select nodes.rowid, facility, split, hostname from nodes, facilities where facilities.name = $1 and facility = facilities.rowid and hostname = $2";
    let rows = dbc
        .query(sql, &[&node_config.node_config.cluster.backend, &node_config.node.host])
        .await
        .errconv()?;
    if rows.len() != 1 {
        return Err(Error::with_msg(format!(
            "get_node can't find unique entry for {} {}",
            node_config.node.host, node_config.node_config.cluster.backend
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

pub async fn get_node_disk_ident_2(
    node_config: Pin<&NodeConfigCached>,
    dbc: Pin<&Client>,
) -> Result<NodeDiskIdent, Error> {
    let sql = "select nodes.rowid, facility, split, hostname from nodes, facilities where facilities.name = $1 and facility = facilities.rowid and hostname = $2";
    let rows = dbc
        .query(sql, &[&node_config.node_config.cluster.backend, &node_config.node.host])
        .await
        .errconv()?;
    if rows.len() != 1 {
        return Err(Error::with_msg(format!(
            "get_node can't find unique entry for {} {}",
            node_config.node.host, node_config.node_config.cluster.backend
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

#[pin_project]
pub struct FindChannelNamesFromConfigReadDir {
    #[pin]
    read_dir_fut: Option<Pin<Box<dyn Future<Output = std::io::Result<ReadDir>> + Send>>>,
    read_dir: Option<ReadDir>,
    #[pin]
    dir_entry_fut: Option<Pin<Box<dyn Future<Output = std::io::Result<Option<DirEntry>>> + Send>>>,
}

impl FindChannelNamesFromConfigReadDir {
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            read_dir_fut: Some(Box::pin(tokio::fs::read_dir(base_dir.as_ref().join("config")))),
            read_dir: None,
            dir_entry_fut: None,
        }
    }
}

impl Stream for FindChannelNamesFromConfigReadDir {
    type Item = Result<DirEntry, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut pself = self.project();
        loop {
            break if let Some(fut) = pself.dir_entry_fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Ready(Ok(Some(item))) => {
                        let g = unsafe { &mut *(pself.read_dir.as_mut().unwrap() as *mut ReadDir) };
                        let fut = g.next_entry();
                        *pself.dir_entry_fut = Some(Box::pin(fut));
                        Ready(Some(Ok(item)))
                    }
                    Ready(Ok(None)) => {
                        *pself.dir_entry_fut = None;
                        Ready(None)
                    }
                    Ready(Err(e)) => Ready(Some(Err(e.into()))),
                    Pending => Pending,
                }
            } else if let Some(fut) = pself.read_dir_fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Ready(Ok(item)) => {
                        *pself.read_dir_fut = None;
                        *pself.read_dir = Some(item);
                        //let fut = pself.read_dir.as_mut().unwrap().next_entry();
                        let g = unsafe { &mut *(pself.read_dir.as_mut().unwrap() as *mut ReadDir) };
                        let fut = g.next_entry();
                        *pself.dir_entry_fut = Some(Box::pin(fut));
                        continue;
                    }
                    Ready(Err(e)) => Ready(Some(Err(e.into()))),
                    Pending => Pending,
                }
            } else {
                Pending
            };
        }
    }
}

async fn find_channel_names_from_config<F, Fut>(base_dir: impl AsRef<Path>, mut cb: F) -> Result<(), Error>
where
    F: FnMut(&str) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    let path2: PathBuf = base_dir.as_ref().join("config");
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

#[pin_project]
pub struct UpdatedDbWithChannelNamesStream {
    errored: bool,
    data_complete: bool,
    #[allow(dead_code)]
    node_config: Pin<Box<NodeConfigCached>>,
    // TODO can we pass a Pin to the async fn instead of creating static ref?
    node_config_ref: &'static NodeConfigCached,
    #[pin]
    client_fut: Option<Pin<Box<dyn Future<Output = Result<Client, Error>> + Send>>>,
    #[pin]
    client: Option<Client>,
    client_ref: Option<&'static Client>,
    #[pin]
    ident_fut: Option<Pin<Box<dyn Future<Output = Result<NodeDiskIdent, Error>> + Send>>>,
    ident: Option<NodeDiskIdent>,
    #[pin]
    find: Option<FindChannelNamesFromConfigReadDir>,
    #[pin]
    update_batch: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
    channel_inp_done: bool,
    clist: Vec<String>,
}

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
            find: None,
            update_batch: None,
            channel_inp_done: false,
            clist: vec![],
        };
        ret.client_fut = Some(Box::pin(crate::create_connection(
            &ret.node_config_ref.node_config.cluster.database,
        )));
        Ok(ret)
    }
}

impl Stream for UpdatedDbWithChannelNamesStream {
    type Item = Result<UpdatedDbWithChannelNames, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut pself = self.project();
        loop {
            break if *pself.errored {
                Ready(None)
            } else if *pself.data_complete {
                Ready(None)
            } else if let Some(fut) = pself.find.as_mut().as_pin_mut() {
                match fut.poll_next(cx) {
                    Ready(Some(Ok(item))) => {
                        pself
                            .clist
                            .push(String::from_utf8(item.file_name().into_vec()).unwrap());
                        continue;
                    }
                    Ready(Some(Err(e))) => {
                        *pself.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        *pself.channel_inp_done = true;
                        // Work through the collected items
                        let l = std::mem::replace(pself.clist, vec![]);
                        let fut = update_db_with_channel_name_list(
                            l,
                            pself.ident.as_ref().unwrap().facility,
                            pself.client.as_ref().get_ref().as_ref().unwrap(),
                        );
                        // TODO
                        //pself.update_batch.replace(Box::pin(fut));
                        let _ = fut;
                        continue;
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = pself.ident_fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Ready(Ok(item)) => {
                        *pself.ident_fut = None;
                        *pself.ident = Some(item);
                        let ret = UpdatedDbWithChannelNames {
                            msg: format!("Got ident {:?}", pself.ident),
                            count: 43,
                        };
                        let base_path = &pself
                            .node_config
                            .node
                            .sf_databuffer
                            .as_ref()
                            .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
                            .data_base_path;
                        let s = FindChannelNamesFromConfigReadDir::new(base_path);
                        *pself.find = Some(s);
                        Ready(Some(Ok(ret)))
                    }
                    Ready(Err(e)) => {
                        *pself.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = pself.client_fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Ready(Ok(item)) => {
                        *pself.client_fut = None;
                        //*pself.client = Some(Box::pin(item));
                        //*pself.client_ref = Some(unsafe { &*(&pself.client.as_ref().unwrap() as &Client as *const _) });
                        *pself.client = Some(item);
                        let c2: &Client = pself.client.as_ref().get_ref().as_ref().unwrap();
                        *pself.client_ref = Some(unsafe { &*(c2 as *const _) });

                        //() == pself.node_config.as_ref();
                        //() == pself.client.as_ref().as_pin_ref().unwrap();
                        /* *pself.ident_fut = Some(Box::pin(get_node_disk_ident_2(
                            pself.node_config.as_ref(),
                            pself.client.as_ref().as_pin_ref().unwrap(),
                        )));*/
                        *pself.ident_fut = Some(Box::pin(get_node_disk_ident(
                            pself.node_config_ref,
                            pself.client_ref.as_ref().unwrap(),
                        )));
                        let ret = UpdatedDbWithChannelNames {
                            msg: format!("Client opened connection"),
                            count: 42,
                        };
                        Ready(Some(Ok(ret)))
                    }
                    Ready(Err(e)) => {
                        *pself.errored = true;
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

async fn update_db_with_channel_name_list(list: Vec<String>, backend: i64, dbc: &Client) -> Result<(), Error> {
    crate::delay_io_short().await;
    dbc.query("begin", &[]).await.errconv()?;
    for ch in list {
        dbc.query(
            "insert into channels (facility, name) values ($1, $2) on conflict do nothing",
            &[&backend, &ch],
        )
        .await
        .errconv()?;
    }
    dbc.query("commit", &[]).await.errconv()?;
    Ok(())
}

pub async fn update_db_with_channel_names(
    node_config: NodeConfigCached,
    db_config: &Database,
) -> Result<Receiver<Result<UpdatedDbWithChannelNames, Error>>, Error> {
    let (tx, rx) = bounded(16);
    let tx2 = tx.clone();
    let db_config = db_config.clone();
    let block1 = async move {
        let dbc = crate::create_connection(&db_config).await?;
        let node_disk_ident = get_node_disk_ident(&node_config, &dbc).await?;
        let c1 = Arc::new(RwLock::new(0u32));
        dbc.query("begin", &[]).await.errconv()?;
        let dbc = Arc::new(dbc);
        let tx = Arc::new(tx);
        let base_path = &node_config
            .node
            .sf_databuffer
            .as_ref()
            .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
            .data_base_path;
        find_channel_names_from_config(base_path, |ch| {
            let ch = ch.to_owned();
            let dbc = dbc.clone();
            let c1 = c1.clone();
            let tx = tx.clone();
            let fac = node_disk_ident.facility;
            async move {
                crate::delay_io_short().await;
                dbc.query(
                    "insert into channels (facility, name) values ($1, $2) on conflict do nothing",
                    &[&fac, &ch],
                )
                .await
                .errconv()?;
                let c2 = {
                    let mut g = c1.write()?;
                    *g += 1;
                    *g
                };
                if c2 % 200 == 0 {
                    dbc.query("commit", &[]).await.errconv()?;
                    let ret = UpdatedDbWithChannelNames {
                        msg: format!("current {}", ch),
                        count: c2,
                    };
                    tx.send(Ok(ret)).await.errconv()?;
                    crate::delay_io_medium().await;
                    dbc.query("begin", &[]).await.errconv()?;
                }
                Ok(())
            }
        })
        .await?;
        dbc.query("commit", &[]).await.errconv()?;
        let c2 = *c1.read()?;
        let ret = UpdatedDbWithChannelNames {
            msg: format!("all done"),
            count: c2,
        };
        tx.send(Ok(ret)).await.errconv()?;
        Ok::<_, Error>(())
    };
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx2.send(Err(e)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not report error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(rx)
}

pub fn update_db_with_channel_names_3<'a>(
    node_config: &'a NodeConfigCached,
) -> impl Stream<Item = Result<UpdatedDbWithChannelNames, Error>> + 'static {
    let base_path = &node_config
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))
        .unwrap()
        .data_base_path;
    futures_util::future::ready(base_path.clone())
        .then(|path| tokio::fs::read_dir(path))
        .map(Result::unwrap)
        .map(|rd| {
            futures_util::stream::unfold(rd, move |rd| {
                //let fut = rd.next_entry();
                futures_util::future::ready(Ok(None)).map(move |item: Result<Option<u32>, Error>| match item {
                    Ok(Some(item)) => Some((item, rd)),
                    Ok(None) => None,
                    Err(_e) => None,
                })
            })
        })
        .map(|_conf| Err(Error::with_msg("TODO")))
        .into_stream()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatedDbWithAllChannelConfigs {
    msg: String,
    count: u32,
}

pub async fn update_db_with_all_channel_configs(
    node_config: NodeConfigCached,
) -> Result<Receiver<Result<UpdatedDbWithAllChannelConfigs, Error>>, Error> {
    let (tx, rx) = bounded(16);
    let tx = Arc::new(tx);
    let tx2 = tx.clone();
    let tx3 = tx.clone();
    let block1 = async move {
        let node_config = &node_config;
        let dbc = crate::create_connection(&node_config.node_config.cluster.database).await?;
        let dbc = Arc::new(dbc);
        let node_disk_ident = &get_node_disk_ident(node_config, &dbc).await?;
        let rows = dbc
            .query(
                "select rowid, facility, name from channels where facility = $1 order by facility, name",
                &[&node_disk_ident.facility],
            )
            .await
            .errconv()?;
        let mut c1 = 0;
        dbc.query("begin", &[]).await.errconv()?;
        let mut count_inserted = 0;
        let mut count_updated = 0;
        for row in rows {
            let rowid: i64 = row.try_get(0).errconv()?;
            let _facility: i64 = row.try_get(1).errconv()?;
            let channel: String = row.try_get(2).errconv()?;
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
                Err(e) => {
                    error!("{:?}", e);
                    crate::delay_io_medium().await;
                }
                Ok(UpdateChannelConfigResult::NotFound) => {
                    warn!("can not find channel config {}", channel);
                    crate::delay_io_medium().await;
                }
                Ok(UpdateChannelConfigResult::Done) => {
                    c1 += 1;
                    if c1 % 200 == 0 {
                        dbc.query("commit", &[]).await.errconv()?;
                        let msg = format!(
                            "channel no {:6}  inserted {:6}  updated {:6}",
                            c1, count_inserted, count_updated
                        );
                        let ret = UpdatedDbWithAllChannelConfigs { msg, count: c1 };
                        tx.send(Ok(ret)).await.errconv()?;
                        dbc.query("begin", &[]).await.errconv()?;
                    }
                    crate::delay_io_short().await;
                }
            }
        }
        dbc.query("commit", &[]).await.errconv()?;
        let msg = format!(
            "ALL DONE  channel no {:6}  inserted {:6}  updated {:6}",
            c1, count_inserted, count_updated
        );
        let ret = UpdatedDbWithAllChannelConfigs { msg, count: c1 };
        tx.send(Ok(ret)).await.errconv()?;
        Ok::<_, Error>(())
    }
    .then({
        |item| async move {
            match item {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("Seeing error: {:?}", e);
                    let ret = UpdatedDbWithAllChannelConfigs { msg, count: 0 };
                    tx2.send(Ok(ret)).await.errconv()?;
                }
            }
            Ok::<_, Error>(())
        }
    });
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx3.send(Err(e)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not deliver error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(rx)
}

pub async fn update_search_cache(node_config: &NodeConfigCached) -> Result<(), Error> {
    let dbc = crate::create_connection(&node_config.node_config.cluster.database).await?;
    dbc.query("select update_cache()", &[]).await.errconv()?;
    Ok(())
}

pub enum UpdateChannelConfigResult {
    NotFound,
    Done,
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
) -> Result<UpdateChannelConfigResult, Error> {
    let base_path = &node_config
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
        .data_base_path;
    let path = base_path
        .join("config")
        .join(channel)
        .join("latest")
        .join("00000_Config");
    let meta = if let Ok(k) = tokio::fs::metadata(&path).await {
        k
    } else {
        return Ok(UpdateChannelConfigResult::NotFound);
    };
    if meta.len() > 8 * 1024 * 1024 {
        return Err(Error::with_msg("meta data too long"));
    }
    let rows = dbc
        .query(
            "select rowid, fileSize, parsedUntil, channel from configs where node = $1 and channel = $2",
            &[&node_disk_ident.rowid(), &channel_id],
        )
        .await
        .errconv()?;
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
            let sql = concat!(
            "insert into configs_history (rowid_original, node, channel, fileSize, parsedUntil, config, tsinsert) ",
            "select rowid as rowid_original, node, channel, fileSize, parsedUntil, config, now() from configs where rowid = $1"
            );
            dbc.query(sql, &[&rowid]).await.errconv()?;
        }
        //ensure!(meta.len() >= parsed_until as u64, ConfigFileOnDiskShrunk{path});
        (Some(rowid), true)
    } else {
        (None, true)
    };
    if do_parse {
        let buf = tokio::fs::read(&path).await?;
        let config = parse::channelconfig::parse_config(&buf).map_err(NErr::from)?.1;
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
                .await
                .errconv()?;
                *count_inserted += 1;
            }
            Some(_config_id_2) => {
                dbc.query(
                    "insert into configs (node, channel, fileSize, parsedUntil, config) values ($1, $2, $3, $4, $5) on conflict (node, channel) do update set fileSize = $3, parsedUntil = $4, config = $5",
                    &[&node_disk_ident.rowid(), &channel_id, &(meta.len() as i64), &(buf.len() as i64), &serde_json::to_value(config)?],
                ).await.errconv()?;
                *count_updated += 1;
            }
        }
    }
    Ok(UpdateChannelConfigResult::Done)
}

pub async fn update_db_with_all_channel_datafiles(
    node_config: &NodeConfigCached,
    node_disk_ident: &NodeDiskIdent,
    ks_prefix: &str,
) -> Result<(), Error> {
    let dbc = Arc::new(crate::create_connection(&node_config.node_config.cluster.database).await?);
    let rows = dbc
        .query(
            "select rowid, facility, name from channels where facility = $1 order by facility, name",
            &[&node_disk_ident.facility()],
        )
        .await
        .errconv()?;
    let mut c1 = 0;
    dbc.query("begin", &[]).await.errconv()?;
    for row in rows {
        let rowid: i64 = row.try_get(0).errconv()?;
        let _facility: i64 = row.try_get(1).errconv()?;
        let channel: String = row.try_get(2).errconv()?;
        update_db_with_channel_datafiles(node_config, node_disk_ident, ks_prefix, rowid, &channel, dbc.clone()).await?;
        c1 += 1;
        if c1 % 40 == 0 {
            trace!("import datafiles  {}  {}", c1, channel);
            dbc.query("commit", &[]).await.errconv()?;
            dbc.query("begin", &[]).await.errconv()?;
        }
        if false && c1 >= 30 {
            break;
        }
    }
    dbc.query("commit", &[]).await.errconv()?;
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
            ).await.errconv()?;
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
    let base_path = &node_config
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
        .data_base_path;
    let writer = DatafileDbWriter {
        node_id: node_disk_ident.rowid(),
        channel_id: channel_id,
        dbc: dbc.clone(),
        c1: Arc::new(RwLock::new(0)),
    };
    let mut n_nothing = 0;
    for ks in &[2, 3, 4] {
        match find_channel_datafiles_in_ks(base_path, ks_prefix, *ks, channel, &writer).await {
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
