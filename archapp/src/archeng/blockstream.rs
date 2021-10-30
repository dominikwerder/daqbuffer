use crate::archeng::datablock::{read_data_1, read_datafile_header};
use crate::archeng::indexfiles::{database_connect, unfold_stream, UnfoldExec};
use crate::archeng::indextree::{read_datablockref, IndexFileBasics, RecordIter, RecordTarget};
use crate::archeng::{open_read, seek, StatsChannel};
use err::Error;
use futures_core::{Future, Stream};
#[allow(unused)]
use netpod::log::*;
use netpod::{Channel, ChannelArchiver, FilePos, NanoRange};
use serde_json::Value as JsVal;
use std::collections::{BTreeMap, VecDeque};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::fs::File;

use super::indextree::HeaderVersion;

enum Steps {
    Start,
    SelectIndexFile,
    SetupNextPath,
    ReadBlocks(RecordIter, Box<dyn HeaderVersion>, PathBuf),
    Done,
}

struct DataBlocks {
    conf: ChannelArchiver,
    channel: Channel,
    range: NanoRange,
    steps: Steps,
    paths: VecDeque<String>,
    file1: Option<File>,
    file2: Option<File>,
    last_dp: u64,
    last_dp2: u64,
    last_f2: String,
    dfnotfound: BTreeMap<String, bool>,
}

impl DataBlocks {
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
            dfnotfound: BTreeMap::new(),
        }
    }

    async fn exec(mut self) -> Result<Option<(JsVal, Self)>, Error> {
        use Steps::*;
        match self.steps {
            Start => {
                self.steps = SelectIndexFile;
                Ok(Some((JsVal::Null, self)))
            }
            SelectIndexFile => {
                let dbc = database_connect(&self.conf.database).await?;
                let sql = "select path from indexfiles i, channels c, channel_index_map m where c.name = $1 and m.channel = c.rowid and i.rowid = m.index";
                let rows = dbc.query(sql, &[&self.channel.name()]).await?;
                for row in rows {
                    self.paths.push_back(row.try_get(0)?);
                }
                self.steps = SetupNextPath;
                Ok(Some((JsVal::String(format!("INIT")), self)))
            }
            SetupNextPath => {
                let stats = &StatsChannel::dummy();
                // For simplicity, simply read all storage classes linearly.
                if let Some(path) = self.paths.pop_front() {
                    // TODO
                    let basics = IndexFileBasics::from_path(&path, stats).await?;
                    let mut tree = basics
                        .rtree_for_channel(self.channel.name(), stats)
                        .await?
                        .ok_or_else(|| Error::with_msg_no_trace("channel not in index files"))?;
                    if let Some(iter) = tree.iter_range(self.range.clone(), stats).await? {
                        self.steps = ReadBlocks(iter, basics.hver().duplicate(), path.clone().into());
                        self.file1 = Some(open_read(path.into(), stats).await?);
                    } else {
                        self.steps = SetupNextPath;
                    };
                } else {
                    self.steps = Done;
                }
                Ok(Some((JsVal::String(format!("NEXTPATH")), self)))
            }
            ReadBlocks(ref mut iter, ref hver, ref indexpath) => {
                // TODO stats
                let stats = &StatsChannel::dummy();
                // TODO I need to keep some datafile open.
                let item = if let Some(rec) = iter.next().await? {
                    // TODO the iterator should actually return Dataref. We never expect child nodes here.
                    if let RecordTarget::Dataref(dp) = rec.target {
                        let f1 = self.file1.as_mut().unwrap();
                        //seek(f1, SeekFrom::Start(dp.0), stats).await?;
                        // Read the dataheader...
                        // TODO the function should take a DatarefPos or?
                        // TODO the seek is hidden in the function which makes possible optimization not accessible.
                        let dref = read_datablockref(f1, FilePos { pos: dp.0 }, hver.as_ref(), stats).await?;
                        // TODO Remember the index path, need it here for relative path.
                        // TODO open datafile, relative path to index path.
                        // TODO keep open when path does not change.
                        let acc;
                        let num_samples;
                        if let Some(_) = self.dfnotfound.get(dref.file_name()) {
                            num_samples = 0;
                            acc = 1;
                        } else {
                            if dref.file_name() == self.last_f2 {
                                acc = 2;
                            } else {
                                let dpath = indexpath.parent().unwrap().join(dref.file_name());
                                match open_read(dpath, stats).await {
                                    Ok(f2) => {
                                        acc = 4;
                                        self.file2 = Some(f2);
                                        self.last_f2 = dref.file_name().into();
                                    }
                                    Err(_) => {
                                        acc = 3;
                                        self.file2 = None;
                                    }
                                }
                            };
                            if let Some(f2) = self.file2.as_mut() {
                                let dfheader = read_datafile_header(f2, dref.data_header_pos(), stats).await?;
                                num_samples = dfheader.num_samples;
                            } else {
                                self.dfnotfound.insert(dref.file_name().into(), true);
                                num_samples = 0;
                            };
                        }
                        let item = serde_json::to_value((
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
                        item
                    } else {
                        panic!();
                    }
                } else {
                    self.steps = SetupNextPath;
                    JsVal::String(format!("NOMORE"))
                };
                Ok(Some((item, self)))
            }
            Done => Ok(None),
        }
    }
}

impl UnfoldExec for DataBlocks {
    type Output = JsVal;

    fn exec(self) -> Pin<Box<dyn Future<Output = Result<Option<(Self::Output, Self)>, Error>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(self.exec())
    }
}

pub fn blockstream(
    channel: Channel,
    range: NanoRange,
    conf: ChannelArchiver,
) -> impl Stream<Item = Result<JsVal, Error>> {
    unfold_stream(DataBlocks::new(channel, range, conf.clone()))
}
