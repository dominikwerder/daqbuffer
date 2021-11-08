use crate::archeng::indexfiles::database_connect;
use err::Error;
use futures_core::{Future, Stream};
use futures_util::{FutureExt, StreamExt};
use netpod::log::*;
use netpod::{Channel, ChannelArchiver, ChannelConfigQuery, ChannelConfigResponse, Database, NanoRange};
use serde::Serialize;
use serde_json::Value as JsVal;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio_postgres::{Client, Row};

pub struct ChannelNameStream {
    db_config: Database,
    max_name: String,
    db_done: bool,
    batch: VecDeque<String>,
    connect_fut: Option<Pin<Box<dyn Future<Output = Result<Client, Error>> + Send>>>,
    select_fut: Option<Pin<Box<dyn Future<Output = Result<Vec<Row>, Error>> + Send>>>,
    done: bool,
    complete: bool,
}

impl ChannelNameStream {
    pub fn new(db_config: Database) -> Self {
        Self {
            db_config,
            max_name: String::new(),
            db_done: false,
            batch: VecDeque::new(),
            connect_fut: None,
            select_fut: None,
            done: false,
            complete: false,
        }
    }
}

impl Stream for ChannelNameStream {
    type Item = Result<String, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if let Some(item) = self.batch.pop_front() {
                Ready(Some(Ok(item)))
            } else if let Some(fut) = &mut self.select_fut {
                match fut.poll_unpin(cx) {
                    Ready(Ok(rows)) => {
                        self.select_fut = None;
                        if rows.len() == 0 {
                            self.db_done = true;
                        }
                        if let Some(last) = rows.last().as_ref() {
                            self.max_name = last.get(1);
                        }
                        for row in rows {
                            self.batch.push_back(row.get(1));
                        }
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.select_fut = None;
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = &mut self.connect_fut {
                match fut.poll_unpin(cx) {
                    Ready(Ok(dbc)) => {
                        self.connect_fut = None;
                        let max_name = self.max_name.clone();
                        info!("select channels  max_name {}", max_name);
                        let fut = async move {
                            let rows = dbc
                                .query(
                                    "select rowid, name from channels where config = '{}'::jsonb and name > $1 order by name limit 64",
                                    &[&max_name],
                                )
                                .await?;
                            Ok::<_, Error>(rows)
                        };
                        self.select_fut = Some(Box::pin(fut));
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.connect_fut = None;
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else {
                if self.db_done {
                    self.done = true;
                    info!("db_done");
                    continue;
                } else {
                    let db = self.db_config.clone();
                    let fut = async move { database_connect(&db).await };
                    self.connect_fut = Some(Box::pin(fut));
                    continue;
                }
            };
        }
    }
}

enum Res {
    TimedOut(String),
    Response(ChannelConfigResponse),
}

#[derive(Debug, Serialize)]
pub enum ConfigItem {
    Config(ChannelConfigResponse),
    JsVal(JsVal),
}

pub struct ConfigStream {
    conf: ChannelArchiver,
    inp: ChannelNameStream,
    inp_done: bool,
    get_fut: Option<Pin<Box<dyn Future<Output = Result<Res, Error>> + Send>>>,
    update_fut: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
    done: bool,
    complete: bool,
}

impl ConfigStream {
    pub fn new(inp: ChannelNameStream, conf: ChannelArchiver) -> Self {
        Self {
            conf,
            inp,
            inp_done: false,
            get_fut: None,
            update_fut: None,
            done: false,
            complete: false,
        }
    }
}

impl Stream for ConfigStream {
    type Item = Result<ConfigItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if let Some(fut) = &mut self.update_fut {
                match fut.poll_unpin(cx) {
                    Ready(Ok(_)) => {
                        self.update_fut = None;
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.update_fut = None;
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = &mut self.get_fut {
                match fut.poll_unpin(cx) {
                    Ready(Ok(Res::Response(item))) => {
                        self.get_fut = None;
                        let name = item.channel.name.clone();
                        let dbconf = self.conf.database.clone();
                        let config = serde_json::to_value(&item)?;
                        let fut = async move {
                            let dbc = database_connect(&dbconf).await?;
                            dbc.query("update channels set config = $2 where name = $1", &[&name, &config])
                                .await?;
                            Ok(())
                        };
                        self.update_fut = Some(Box::pin(fut));
                        let item = ConfigItem::Config(item);
                        Ready(Some(Ok(item)))
                    }
                    Ready(Ok(Res::TimedOut(name))) => {
                        self.get_fut = None;
                        let dbconf = self.conf.database.clone();
                        let config = serde_json::to_value(&"TimedOut")?;
                        let fut = async move {
                            let dbc = database_connect(&dbconf).await?;
                            dbc.query("update channels set config = $2 where name = $1", &[&name, &config])
                                .await?;
                            Ok(())
                        };
                        self.update_fut = Some(Box::pin(fut));
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.get_fut = None;
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else {
                if self.inp_done {
                    self.done = true;
                    continue;
                } else {
                    match self.inp.poll_next_unpin(cx) {
                        Ready(Some(Ok(item))) => {
                            let conf = self.conf.clone();
                            let fut = async move {
                                let channel = Channel {
                                    name: item,
                                    backend: "".into(),
                                };
                                let now = SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                let beg = now - 60 * 60 * 1000;
                                let end = now + 60 * 60 * 4;
                                let q = ChannelConfigQuery {
                                    channel,
                                    range: NanoRange { beg, end },
                                };
                                let fut = super::channel_config(&q, &conf);
                                let fut = tokio::time::timeout(Duration::from_millis(2000), fut);
                                match fut.await {
                                    Ok(Ok(k)) => Ok(Res::Response(k)),
                                    Ok(Err(e)) => Err(e),
                                    Err(_) => {
                                        warn!("timeout");
                                        Ok(Res::TimedOut(q.channel.name))
                                    }
                                }
                            };
                            self.get_fut = Some(Box::pin(fut));
                            continue;
                        }
                        Ready(Some(Err(e))) => {
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                        Ready(None) => {
                            self.inp_done = true;
                            info!("ConfigStream  input done.");
                            continue;
                        }
                        Pending => Pending,
                    }
                }
            };
        }
    }
}
