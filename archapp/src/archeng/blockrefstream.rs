use crate::archeng::backreadbuf::BackReadBuf;
use crate::archeng::datablock::{read_data2, read_datafile_header2};
use crate::archeng::indexfiles::{database_connect, unfold_stream, UnfoldExec};
use crate::archeng::indextree::{
    read_datablockref2, DataheaderPos, Dataref, HeaderVersion, IndexFileBasics, RecordIter, RecordTarget,
};
use commonio::ringbuf::RingBuf;
use commonio::{open_read, StatsChannel};
use err::Error;
use futures_core::{Future, Stream};
use items::WithLen;
#[allow(unused)]
use netpod::log::*;
use netpod::{Channel, ChannelArchiver, NanoRange};
#[allow(unused)]
use serde::Serialize;
use serde_json::Value as JsVal;
use std::collections::{BTreeMap, VecDeque};
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
    conf: ChannelArchiver,
    channel: Channel,
    range: NanoRange,
    steps: Steps,
    paths: VecDeque<String>,
    file1: Option<BackReadBuf<File>>,
    file2: Option<RingBuf<File>>,
    last_dp: u64,
    last_dp2: u64,
    last_f2: String,
    last_dfhpos: DataheaderPos,
    dfnotfound: BTreeMap<String, bool>,
    data_bytes_read: u64,
    same_dfh_count: u64,
}

impl BlockrefStream {
    fn new(channel: Channel, range: NanoRange, conf: ChannelArchiver) -> Self {
        Self {
            conf,
            channel,
            range,
            steps: Steps::Start,
            paths: VecDeque::new(),
            file1: None,
            file2: None,
            last_dp: 0,
            last_dp2: 0,
            last_f2: String::new(),
            last_dfhpos: DataheaderPos(u64::MAX),
            dfnotfound: BTreeMap::new(),
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
                let dbc = database_connect(&self.conf.database).await?;
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
                    let mut file = open_read(path.clone().into(), stats).await?;
                    let basics = IndexFileBasics::from_file(&path, &mut file, stats).await?;
                    let mut tree = basics
                        .rtree_for_channel(self.channel.name(), stats)
                        .await?
                        .ok_or_else(|| Error::with_msg_no_trace("channel not in index files"))?;
                    if let Some(iter) = tree.iter_range(self.range.clone(), stats).await? {
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
                // TODO stats
                let stats = &StatsChannel::dummy();
                // TODO I need to keep some datafile open.
                let item = if let Some(rec) = iter.next().await? {
                    // TODO the iterator should actually return Dataref. We never expect child nodes here.
                    if let RecordTarget::Dataref(dp) = rec.target {
                        let f1 = self.file1.as_mut().unwrap();
                        let dref = read_datablockref2(f1, dp.clone(), hver.as_ref()).await?;
                        let dpath = indexpath.parent().unwrap().join(dref.file_name());
                        // TODO Remember the index path, need it here for relative path.
                        // TODO open datafile, relative path to index path.
                        // TODO keep open when path does not change.
                        let acc;
                        let num_samples;
                        if false {
                            if let Some(_) = self.dfnotfound.get(dref.file_name()) {
                                num_samples = 0;
                                acc = 1;
                            } else {
                                if dref.file_name() == self.last_f2 {
                                    acc = 2;
                                } else {
                                    match open_read(dpath.clone(), stats).await {
                                        Ok(f2) => {
                                            acc = 4;
                                            self.file2 = Some(
                                                RingBuf::new(f2, dref.data_header_pos().0, StatsChannel::dummy())
                                                    .await?,
                                            );
                                            self.last_f2 = dref.file_name().into();
                                        }
                                        Err(_) => {
                                            acc = 3;
                                            self.file2 = None;
                                        }
                                    }
                                };
                                if let Some(f2) = self.file2.as_mut() {
                                    if dref.file_name() == self.last_f2 && dref.data_header_pos() == self.last_dfhpos {
                                        num_samples = 0;
                                    } else {
                                        self.last_dfhpos = dref.data_header_pos();
                                        let rp1 = f2.rp_abs();
                                        let dfheader = read_datafile_header2(f2, dref.data_header_pos()).await?;
                                        let data = read_data2(f2, &dfheader, self.range.clone(), false).await?;
                                        let rp2 = f2.rp_abs();
                                        self.data_bytes_read += rp2 - rp1;
                                        num_samples = dfheader.num_samples;
                                        if data.len() != num_samples as usize {
                                            if (data.len() as i64 - num_samples as i64).abs() < 4 {
                                                // TODO get always one event less than num_samples tells us.
                                                //warn!("small deviation  {} vs {}", data.len(), num_samples);
                                            } else {
                                                return Err(Error::with_msg_no_trace(format!(
                                                    "event count mismatch  {} vs {}",
                                                    data.len(),
                                                    num_samples
                                                )));
                                            }
                                        }
                                    }
                                } else {
                                    self.dfnotfound.insert(dref.file_name().into(), true);
                                    num_samples = 0;
                                };
                            }
                        } else {
                            acc = 6;
                            num_samples = 0;
                        }
                        let jsval = serde_json::to_value((
                            dp.0,
                            dp.0 as i64 - self.last_dp as i64,
                            dref.file_name(),
                            dref.data_header_pos.0,
                            dref.data_header_pos.0 as i64 - self.last_dp2 as i64,
                            dref.next().0,
                            acc,
                            num_samples,
                        ))?;
                        self.last_dp = dp.0;
                        self.last_dp2 = dref.data_header_pos.0;
                        let bref = Blockref { dref, dpath };
                        BlockrefItem::Blockref(bref, jsval)
                    } else {
                        panic!();
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
    conf: ChannelArchiver,
) -> impl Stream<Item = Result<BlockrefItem, Error>> {
    unfold_stream(BlockrefStream::new(channel, range, conf.clone()))
}
