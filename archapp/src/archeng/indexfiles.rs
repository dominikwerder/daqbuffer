use crate::timed::Timed;
use crate::wrap_task;
use async_channel::Receiver;
use commonio::{open_read, read, StatsChannel};
use err::Error;
use futures_core::{Future, Stream};
use futures_util::stream::unfold;
use netpod::log::*;
use netpod::{Channel, ChannelArchiver, Database};
use regex::Regex;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::read_dir;
use tokio::sync::Mutex;
use tokio_postgres::Client as PgClient;

pub fn list_index_files(node: &ChannelArchiver) -> Receiver<Result<PathBuf, Error>> {
    let node = node.clone();
    let (tx, rx) = async_channel::bounded(4);
    let tx2 = tx.clone();
    let task = async move {
        for bp in &node.data_base_paths {
            let mut rd = read_dir(bp).await?;
            while let Some(e) = rd.next_entry().await? {
                let ft = e.file_type().await?;
                if ft.is_dir() {
                    let mut rd = read_dir(e.path()).await?;
                    while let Some(e) = rd.next_entry().await? {
                        let ft = e.file_type().await?;
                        if false && ft.is_dir() {
                            let mut rd = read_dir(e.path()).await?;
                            while let Some(e) = rd.next_entry().await? {
                                let ft = e.file_type().await?;
                                if ft.is_file() {
                                    if e.file_name().to_string_lossy() == "index" {
                                        tx.send(Ok(e.path())).await?;
                                    }
                                }
                            }
                        } else if ft.is_file() {
                            if e.file_name().to_string_lossy() == "index" {
                                tx.send(Ok(e.path())).await?;
                            }
                        }
                    }
                } else if ft.is_file() {
                    if e.file_name().to_string_lossy() == "index" {
                        tx.send(Ok(e.path())).await?;
                    }
                }
            }
        }
        Ok::<_, Error>(())
    };
    wrap_task(task, tx2);
    rx
}

pub struct ScanIndexFiles0 {}

impl Stream for ScanIndexFiles0 {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let _ = cx;
        todo!()
    }
}

pub async fn get_level_0(conf: ChannelArchiver) -> Result<Vec<PathBuf>, Error> {
    let mut ret = vec![];
    for bp in &conf.data_base_paths {
        let mut rd = read_dir(bp).await?;
        while let Some(e) = rd.next_entry().await? {
            if e.file_name().to_string_lossy().contains("index") {
                warn!("Top-level data path contains `index` entry");
            }
            let ft = e.file_type().await?;
            if ft.is_dir() {
                ret.push(e.path());
            }
        }
    }
    Ok(ret)
}

pub async fn get_level_1(lev0: Vec<PathBuf>) -> Result<Vec<PathBuf>, Error> {
    let mut ret = vec![];
    for bp in lev0 {
        let mut rd = read_dir(bp).await?;
        while let Some(e) = rd.next_entry().await? {
            let ft = e.file_type().await?;
            if ft.is_file() {
                if e.file_name().to_string_lossy() == "index" {
                    ret.push(e.path());
                }
            }
        }
    }
    Ok(ret)
}

pub async fn database_connect(db_config: &Database) -> Result<PgClient, Error> {
    let d = db_config;
    let uri = format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, 5432, d.name);
    let (cl, conn) = tokio_postgres::connect(&uri, tokio_postgres::NoTls).await?;
    // TODO monitor connection drop.
    let _cjh = tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {}", e);
        }
        Ok::<_, Error>(())
    });
    Ok(cl)
}

pub trait UnfoldExec {
    type Output: Send;
    fn exec(self) -> Pin<Box<dyn Future<Output = Result<Option<(Self::Output, Self)>, Error>> + Send>>
    where
        Self: Sized;
}

pub fn unfold_stream<St, T>(st: St) -> impl Stream<Item = Result<T, Error>>
where
    St: UnfoldExec<Output = T> + Send,
    T: Send,
{
    enum UnfoldState<St> {
        Running(St),
        Done,
    }
    unfold(UnfoldState::Running(st), |st| async move {
        match st {
            UnfoldState::Running(st) => match st.exec().await {
                Ok(Some((item, st))) => Some((Ok(item), UnfoldState::Running(st))),
                Ok(None) => None,
                Err(e) => Some((Err(e), UnfoldState::Done)),
            },
            UnfoldState::Done => None,
        }
    })
}

enum ScanIndexFilesSteps {
    Level0,
    Level1(Vec<PathBuf>),
    Done,
}

struct ScanIndexFiles {
    conf: ChannelArchiver,
    steps: ScanIndexFilesSteps,
}

impl ScanIndexFiles {
    fn new(conf: ChannelArchiver) -> Self {
        Self {
            conf,
            steps: ScanIndexFilesSteps::Level0,
        }
    }

    async fn exec(mut self) -> Result<Option<(String, Self)>, Error> {
        match self.steps {
            ScanIndexFilesSteps::Level0 => {
                let res = get_level_0(self.conf.clone()).await?;
                self.steps = ScanIndexFilesSteps::Level1(res);
                let item = format!("level 0 done");
                Ok(Some((item, self)))
            }
            ScanIndexFilesSteps::Level1(paths) => {
                let paths = get_level_1(paths).await?;
                info!("collected {} level 1 paths", paths.len());
                let dbc = database_connect(&self.conf.database).await?;
                for p in paths {
                    let ps = p.to_string_lossy();
                    let rows = dbc
                        .query("select rowid from indexfiles where path = $1", &[&ps])
                        .await?;
                    let rid: i64 = if rows.len() == 0 {
                        let rows = dbc
                            .query(
                                "insert into indexfiles (path) values ($1) on conflict do nothing returning rowid",
                                &[&ps],
                            )
                            .await?;
                        if rows.len() == 0 {
                            error!("insert failed, maybe concurrent insert?");
                            // TODO try this channel again? or the other process handled it?
                            err::todoval()
                        } else if rows.len() == 1 {
                            let rid = rows[0].try_get(0)?;
                            info!("insert done: {}", rid);
                            rid
                        } else {
                            return Err(Error::with_msg("not unique"));
                        }
                    } else if rows.len() == 1 {
                        let rid = rows[0].try_get(0)?;
                        rid
                    } else {
                        return Err(Error::with_msg("not unique"));
                    };
                    let _ = rid;
                }
                self.steps = ScanIndexFilesSteps::Done;
                let item = format!("level 1 done");
                Ok(Some((item, self)))
            }
            ScanIndexFilesSteps::Done => Ok(None),
        }
    }
}

impl UnfoldExec for ScanIndexFiles {
    type Output = String;

    fn exec(self) -> Pin<Box<dyn Future<Output = Result<Option<(Self::Output, Self)>, Error>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(self.exec())
    }
}

pub fn scan_index_files(conf: ChannelArchiver) -> impl Stream<Item = Result<String, Error>> {
    unfold_stream(ScanIndexFiles::new(conf.clone()))
    /*
    enum UnfoldState {
        Running(ScanIndexFiles),
        Done,
    }
    unfold(UnfoldState::Running(ScanIndexFiles::new(conf)), |st| async move {
        match st {
            UnfoldState::Running(st) => match st.exec().await {
                Ok(Some((item, st))) => Some((Ok(item), UnfoldState::Running(st))),
                Ok(None) => None,
                Err(e) => {
                    error!("{}", e);
                    Some((Err(e), UnfoldState::Done))
                }
            },
            UnfoldState::Done => None,
        }
    })
    */
}

pub fn unfold1() -> impl Stream<Item = String> {
    unfold(123u32, |st| async move { Some((format!("{}", st), st)) })
}

pub fn unfold2(_conf: ChannelArchiver) -> () {
    /*let f1 = async move {
        let _list = get_level_0(conf).await?;
        let yld = format!("level 0 done");
        let fut = async { Ok(None) };
        Ok(Some((yld, Box::pin(fut))))
    };
    unfold(
        Box::pin(f1) as Pin<Box<dyn Future<Output = Result<Option<(String, _)>, Error>>>>,
        |st| async {
            match st.await {
                Ok(None) => None,
                Ok(Some((item, st))) => {
                    //Some((item, st));
                    //Some((String::new(), Box::pin(async { Ok(None) })))
                    None
                }
                Err(e) => {
                    error!("{}", e);
                    None
                }
            }
        },
    )*/
    err::todoval()
}

// -------------------------------------------------

enum ScanChannelsSteps {
    Start,
    SelectIndexFile,
    ReadChannels(Vec<String>),
    Done,
}

struct ScanChannels {
    conf: ChannelArchiver,
    steps: ScanChannelsSteps,
}

impl ScanChannels {
    fn new(conf: ChannelArchiver) -> Self {
        Self {
            conf,
            steps: ScanChannelsSteps::Start,
        }
    }

    async fn exec(mut self) -> Result<Option<(String, Self)>, Error> {
        use ScanChannelsSteps::*;
        match self.steps {
            Start => {
                self.steps = SelectIndexFile;
                Ok(Some((format!("Start"), self)))
            }
            SelectIndexFile => {
                let dbc = database_connect(&self.conf.database).await?;
                let sql =
                    "select path from indexfiles where ts_last_channel_search < now() - interval '1 hour' limit 1";
                let rows = dbc.query(sql, &[]).await?;
                let mut paths = vec![];
                for row in rows {
                    paths.push(row.get::<_, String>(0));
                }
                let item = format!("SelectIndexFile {:?}", paths);
                self.steps = ReadChannels(paths);
                Ok(Some((item, self)))
            }
            ReadChannels(mut paths) => {
                // TODO stats
                let stats = &StatsChannel::dummy();
                let dbc = database_connect(&self.conf.database).await?;
                if let Some(path) = paths.pop() {
                    let rows = dbc
                        .query("select rowid from indexfiles where path = $1", &[&path])
                        .await?;
                    if rows.len() == 1 {
                        let indexfile_rid: i64 = rows[0].try_get(0)?;
                        let mut file = open_read(path.clone().into(), stats).await?;
                        let mut basics = super::indextree::IndexFileBasics::from_file(path, &mut file, stats).await?;
                        let entries = basics.all_channel_entries(&mut file, stats).await?;
                        for entry in entries {
                            let rows = dbc
                                .query("select rowid from channels where name = $1", &[&entry.channel_name()])
                                .await?;
                            let rid: i64 = if rows.len() == 0 {
                                let rows = dbc
                                .query(
                                    "insert into channels (name) values ($1) on conflict do nothing returning rowid",
                                    &[&entry.channel_name()],
                                )
                                .await?;
                                if rows.len() == 0 {
                                    error!("insert failed, maybe concurrent insert?");
                                    // TODO try this channel again? or the other process handled it?
                                    err::todoval()
                                } else if rows.len() == 1 {
                                    let rid = rows[0].try_get(0)?;
                                    info!("insert done: {}", rid);
                                    rid
                                } else {
                                    return Err(Error::with_msg("not unique"));
                                }
                            } else if rows.len() == 1 {
                                let rid = rows[0].try_get(0)?;
                                rid
                            } else {
                                return Err(Error::with_msg("not unique"));
                            };
                            dbc.query(
                                "insert into channel_index_map (channel, index) values ($1, $2) on conflict do nothing",
                                &[&rid, &indexfile_rid],
                            )
                            .await?;
                        }
                        dbc.query(
                            "update indexfiles set ts_last_channel_search = now() where rowid = $1",
                            &[&indexfile_rid],
                        )
                        .await?;
                    }
                }
                self.steps = Done;
                Ok(Some((format!("ReadChannels"), self)))
            }
            Done => Ok(None),
        }
    }
}

impl UnfoldExec for ScanChannels {
    type Output = String;

    fn exec(self) -> Pin<Box<dyn Future<Output = Result<Option<(Self::Output, Self)>, Error>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(self.exec())
    }
}

pub fn scan_channels(conf: ChannelArchiver) -> impl Stream<Item = Result<String, Error>> {
    unfold_stream(ScanChannels::new(conf.clone()))
}

#[derive(Debug)]
enum RetClass {
    Long,
    Medium,
    Short,
    #[allow(unused)]
    PostMortem,
}

#[derive(Debug)]
enum IndexCat {
    Machine { rc: RetClass },
    Beamline { rc: RetClass, name: String },
}

#[derive(Debug)]
struct IndexFile {
    path: PathBuf,
    cat: IndexCat,
}

// Try to make sense of historical conventions how the epics channel archiver engines are configured.
fn categorize_index_files(list: &Vec<String>) -> Result<Vec<IndexFile>, Error> {
    let re_m = Regex::new(r"/archive_(ST|MT|LT)/index").unwrap();
    let re_b = Regex::new(r"/archive_(X([0-9]+)[^_]*)_(SH|LO)/index").unwrap();
    let mut ret = vec![];
    for p in list {
        match re_m.captures(p) {
            Some(cap) => {
                let rc = cap.get(1).unwrap().as_str();
                let rc = match rc {
                    "ST" => Some(RetClass::Short),
                    "MT" => Some(RetClass::Medium),
                    "LT" => Some(RetClass::Long),
                    _ => {
                        warn!("categorize_index_files  no idea about RC for {}", p);
                        None
                    }
                };
                if let Some(rc) = rc {
                    let f = IndexFile {
                        path: p.into(),
                        cat: IndexCat::Machine { rc },
                    };
                    ret.push(f);
                }
            }
            None => match re_b.captures(p) {
                Some(cap) => {
                    let name = cap.get(1).unwrap().as_str();
                    let rc = cap.get(3).unwrap().as_str();
                    let rc = match rc {
                        "SH" => Some(RetClass::Short),
                        "LO" => Some(RetClass::Long),
                        _ => {
                            warn!("categorize_index_files  no idea about RC for {}", p);
                            None
                        }
                    };
                    if let Some(rc) = rc {
                        let f = IndexFile {
                            path: p.into(),
                            cat: IndexCat::Beamline { name: name.into(), rc },
                        };
                        ret.push(f);
                    }
                }
                None => {
                    warn!("categorize_index_files  no idea at all about {}", p);
                }
            },
        }
    }
    let is_machine = {
        let mut k = false;
        for x in &ret {
            if let IndexCat::Machine { .. } = &x.cat {
                k = true;
                break;
            }
        }
        k
    };
    // TODO by default, filter post-mortem.
    let is_beamline = !is_machine;
    if is_beamline {
        let mut ret: Vec<_> = ret
            .into_iter()
            .filter_map(|k| {
                if let IndexCat::Machine { rc, .. } = &k.cat {
                    let prio = match rc {
                        &RetClass::Short => 4,
                        &RetClass::Medium => 6,
                        &RetClass::Long => 8,
                        &RetClass::PostMortem => 0,
                    };
                    Some((k, prio))
                } else {
                    None
                }
            })
            .collect();
        ret.sort_by_key(|x| x.1);
        let ret = ret.into_iter().map(|k| k.0).collect();
        Ok(ret)
    } else if is_machine {
        let mut ret: Vec<_> = ret
            .into_iter()
            .filter_map(|k| {
                if let IndexCat::Machine { rc, .. } = &k.cat {
                    let prio = match rc {
                        &RetClass::Short => 4,
                        &RetClass::Medium => 6,
                        &RetClass::Long => 8,
                        &RetClass::PostMortem => 0,
                    };
                    Some((k, prio))
                } else {
                    None
                }
            })
            .collect();
        ret.sort_by_key(|x| x.1);
        let ret = ret.into_iter().map(|k| k.0).collect();
        Ok(ret)
    } else {
        err::todoval()
    }
}

static INDEX_JSON: Mutex<Option<BTreeMap<String, Vec<String>>>> = Mutex::const_new(None);

pub async fn index_files_index_ref<P: Into<PathBuf> + Send>(
    key: &str,
    index_files_index_path: P,
    stats: &StatsChannel,
) -> Result<Option<Vec<String>>, Error> {
    let mut g = INDEX_JSON.lock().await;
    match &*g {
        Some(j) => Ok(j.get(key).map(|x| x.clone())),
        None => {
            let timed1 = Timed::new("slurp_index_json");
            let index_files_index_path = index_files_index_path.into();
            let index_files_index = {
                let timed1 = Timed::new("slurp_index_bytes");
                let mut index_files_index = open_read(index_files_index_path, stats).await?;
                let mut buf = vec![0; 1024 * 1024 * 50];
                let mut ntot = 0;
                loop {
                    let n = read(&mut index_files_index, &mut buf[ntot..], stats).await?;
                    if n == 0 {
                        break;
                    }
                    ntot += n;
                }
                buf.truncate(ntot);
                drop(timed1);
                serde_json::from_slice::<BTreeMap<String, Vec<String>>>(&buf)?
            };
            drop(timed1);
            let ret = index_files_index.get(key).map(|x| x.clone());
            *g = Some(index_files_index);
            Ok(ret)
        }
    }
}

pub async fn index_file_path_list(
    channel: Channel,
    index_files_index_path: PathBuf,
    stats: &StatsChannel,
) -> Result<Vec<PathBuf>, Error> {
    let timed1 = Timed::new("categorize index files");
    let index_paths = index_files_index_ref(channel.name(), &index_files_index_path, stats)
        .await?
        .ok_or(Error::with_msg_no_trace("can not find channel"))?;
    let list = categorize_index_files(&index_paths)?;
    info!("GOT CATEGORIZED:\n{:?}", list);
    let ret = list.into_iter().map(|k| k.path).collect();
    drop(timed1);
    Ok(ret)
}
