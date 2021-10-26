use crate::wrap_task;
use async_channel::Receiver;
use err::Error;
use futures_core::Future;
use futures_core::Stream;
use futures_util::stream::unfold;
use futures_util::FutureExt;
use netpod::log::*;
use netpod::ChannelArchiver;
use netpod::Database;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::read_dir;
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
                    let sql = "insert into indexfiles (path) values ($1) on conflict do nothing";
                    dbc.query(sql, &[&p.to_string_lossy()]).await?;
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
                self.steps = Done;
                Ok(Some((format!("SelectIndexFile {:?}", paths), self)))
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
