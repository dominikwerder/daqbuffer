use crate::archeng::backreadbuf::BackReadBuf;
use crate::archeng::indexfiles::{database_connect, unfold_stream, UnfoldExec};
use crate::archeng::indextree::{
    read_datablockref2, Dataref, HeaderVersion, IndexFileBasics, RecordIter, RecordTarget,
};
use commonio::{open_read, StatsChannel};
use err::Error;
use futures_core::{Future, Stream};
#[allow(unused)]
use netpod::log::*;
use netpod::{Channel, Database, NanoRange};
#[allow(unused)]
use serde::Serialize;
use serde_json::Value as JsVal;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::File;

#[derive(Debug)]
pub struct Blockref {
    pub dref: Dataref,
    pub dpath: PathBuf,
}

#[derive(Debug)]
pub enum BlockrefItem {
    Blockref(Blockref, JsVal),
    JsVal(JsVal),
}

enum Steps {
    Start,
    SelectIndexFile,
    SetupNextPath,
    ReadBlocks(RecordIter, Box<dyn HeaderVersion>, PathBuf),
    Done,
}

struct BlockrefStream {
    dbconf: Database,
    channel: Channel,
    range: NanoRange,
    expand: bool,
    steps: Steps,
    paths: VecDeque<String>,
    file1: Option<BackReadBuf<File>>,
    last_dp: u64,
    last_dp2: u64,
    data_bytes_read: u64,
    same_dfh_count: u64,
}

impl BlockrefStream {
    fn new(channel: Channel, range: NanoRange, expand: bool, dbconf: Database) -> Self {
        debug!("new BlockrefStream  {:?}", range);
        Self {
            dbconf,
            channel,
            range,
            expand,
            steps: Steps::Start,
            paths: VecDeque::new(),
            file1: None,
            last_dp: 0,
            last_dp2: 0,
            data_bytes_read: 0,
            same_dfh_count: 0,
        }
    }

    async fn exec(mut self) -> Result<Option<(BlockrefItem, Self)>, Error> {
        use Steps::*;
        match self.steps {
            Start => {
                self.steps = SelectIndexFile;
                Ok(Some((
                    BlockrefItem::JsVal(JsVal::String(format!("{}  START", module_path!()))),
                    self,
                )))
            }
            SelectIndexFile => {
                let dbc = database_connect(&self.dbconf).await?;
                let sql = "select i.path from indexfiles i, channels c, channel_index_map m where c.name = $1 and m.channel = c.rowid and i.rowid = m.index";
                let rows = dbc.query(sql, &[&self.channel.name()]).await?;
                for row in rows {
                    let p: String = row.try_get(0)?;
                    if self.paths.is_empty() && (p.contains("_ST/") || p.contains("_SH/")) {
                        self.paths.push_back(p);
                    }
                }
                if self.paths.len() == 0 {
                    self.steps = Done;
                    Ok(Some((
                        BlockrefItem::JsVal(JsVal::String(format!("NOPATHSFROMDB"))),
                        self,
                    )))
                } else {
                    self.steps = SetupNextPath;
                    Ok(Some((BlockrefItem::JsVal(JsVal::String(format!("DBQUERY"))), self)))
                }
            }
            SetupNextPath => {
                let stats = &StatsChannel::dummy();
                // For simplicity, simply read all storage classes linearly.
                if let Some(path) = self.paths.pop_front() {
                    // TODO
                    let mut file = open_read(path.clone().into(), stats).await.map_err(|e| {
                        error!("can not open {:?}", path);
                        e
                    })?;
                    let basics = IndexFileBasics::from_file(&path, &mut file, stats).await?;
                    let mut tree = basics
                        .rtree_for_channel(self.channel.name(), stats)
                        .await?
                        .ok_or_else(|| Error::with_msg_no_trace("channel not in index files"))?;
                    if let Some(iter) = tree.iter_range(self.range.clone(), self.expand, stats).await? {
                        debug!("SetupNextPath {:?}", path);
                        self.steps = ReadBlocks(iter, basics.hver().duplicate(), path.clone().into());
                        self.file1 = Some(BackReadBuf::new(file, 0, stats.clone()).await?);
                    } else {
                        self.steps = SetupNextPath;
                    };
                    Ok(Some((BlockrefItem::JsVal(JsVal::String(format!("NEXTPATH"))), self)))
                } else {
                    self.steps = Done;
                    Ok(Some((
                        BlockrefItem::JsVal(JsVal::String(format!("PATHQUEUEEMPTY"))),
                        self,
                    )))
                }
            }
            ReadBlocks(ref mut iter, ref hver, ref indexpath) => {
                let item = if let Some(rec) = iter.next().await? {
                    // TODO the iterator should actually return Dataref. We never expect child nodes here.
                    if let RecordTarget::Dataref(dp) = rec.target {
                        let f1 = self.file1.as_mut().unwrap();
                        let dref = read_datablockref2(f1, dp.clone(), hver.as_ref()).await?;
                        let dpath = indexpath.parent().unwrap().join(dref.file_name());
                        let jsval = serde_json::to_value((
                            dp.0,
                            dp.0 as i64 - self.last_dp as i64,
                            dref.file_name(),
                            dref.data_header_pos.0,
                            dref.data_header_pos.0 as i64 - self.last_dp2 as i64,
                            dref.next().0,
                        ))?;
                        self.last_dp = dp.0;
                        self.last_dp2 = dref.data_header_pos.0;
                        if rec.end.ns > self.range.end {
                            debug!("Have block end beyond range, stop");
                            self.steps = Done;
                        }
                        let bref = Blockref { dref, dpath };
                        trace!("emit {:?}   Record range: {:?} TO {:?}", bref, rec.beg, rec.end);
                        BlockrefItem::Blockref(bref, jsval)
                    } else {
                        error!("not a Dataref target");
                        self.steps = Done;
                        BlockrefItem::JsVal(JsVal::String(format!("not a Dataref target")))
                    }
                } else {
                    debug!(
                        "data_bytes_read: {}  same_dfh_count: {}",
                        self.data_bytes_read, self.same_dfh_count
                    );
                    self.steps = SetupNextPath;
                    BlockrefItem::JsVal(JsVal::String(format!("NOMORE")))
                };
                Ok(Some((item, self)))
            }
            Done => Ok(None),
        }
    }
}

impl UnfoldExec for BlockrefStream {
    type Output = BlockrefItem;

    fn exec(self) -> Pin<Box<dyn Future<Output = Result<Option<(Self::Output, Self)>, Error>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(self.exec())
    }
}

pub fn blockref_stream(
    channel: Channel,
    range: NanoRange,
    expand: bool,
    dbconf: Database,
) -> impl Stream<Item = Result<BlockrefItem, Error>> {
    unfold_stream(BlockrefStream::new(channel, range, expand, dbconf))
}

#[cfg(test)]
mod test {
    use super::*;
    use futures_util::StreamExt;
    use netpod::timeunits::SEC;

    #[test]
    fn find_ref_1() -> Result<(), Error> {
        let fut = async move {
            let channel = Channel {
                backend: "sls-archive".into(),
                name: "X05DA-FE-WI1:TC1".into(),
            };
            use chrono::{DateTime, Utc};
            let dtbeg: DateTime<Utc> = "2021-10-01T00:00:00Z".parse()?;
            let dtend: DateTime<Utc> = "2021-10-10T00:00:00Z".parse()?;
            fn tons(dt: &DateTime<Utc>) -> u64 {
                dt.timestamp() as u64 * SEC + dt.timestamp_subsec_nanos() as u64
            }
            let range = NanoRange {
                beg: tons(&dtbeg),
                end: tons(&dtend),
            };
            let dbconf = Database {
                host: "localhost".into(),
                name: "testingdaq".into(),
                user: "testingdaq".into(),
                pass: "testingdaq".into(),
            };
            let mut refs = Box::pin(blockref_stream(channel, range, false, dbconf));
            while let Some(item) = refs.next().await {
                info!("Got ref  {:?}", item);
            }
            Ok(())
        };
        taskrun::run(fut)
    }
}
