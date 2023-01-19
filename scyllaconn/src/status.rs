use crate::errconv::ErrConv;
use err::Error;
use futures_util::{Future, FutureExt, Stream};
use items_2::channelevents::{ConnStatus, ConnStatusEvent};
use netpod::log::*;
use netpod::NanoRange;
use netpod::CONNECTION_STATUS_DIV;
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

async fn read_next_status_events(
    series: u64,
    ts_msp: u64,
    range: NanoRange,
    fwd: bool,
    do_one_before: bool,
    scy: Arc<ScySession>,
) -> Result<VecDeque<ConnStatusEvent>, Error> {
    if ts_msp >= range.end {
        warn!(
            "given ts_msp {}  >=  range.end {}  not necessary to read this",
            ts_msp, range.end
        );
    }
    if range.end > i64::MAX as u64 {
        return Err(Error::with_msg_no_trace(format!("range.end overflows i64")));
    }
    let res = if fwd {
        let ts_lsp_min = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
        let ts_lsp_max = if ts_msp < range.end { range.end - ts_msp } else { 0 };
        trace!(
            "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  beg {}  end {}",
            ts_msp,
            ts_lsp_min,
            ts_lsp_max,
            range.beg,
            range.end
        );
        // TODO use prepared!
        let cql = concat!(
            "select ts_lsp, kind from channel_status where series = ? and ts_msp = ? and ts_lsp >= ? and ts_lsp < ?"
        );
        scy.query(
            cql,
            (series as i64, ts_msp as i64, ts_lsp_min as i64, ts_lsp_max as i64),
        )
        .await
        .err_conv()?
    } else {
        let ts_lsp_max = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
        debug!(
            "BCK  ts_msp {}  ts_lsp_max {}  beg {}  end {}  {}",
            ts_msp,
            ts_lsp_max,
            range.beg,
            range.end,
            stringify!($fname)
        );
        // TODO use prepared!
        let cql = concat!(
            "select ts_lsp, kind from channel_status where series = ? and ts_msp = ? and ts_lsp < ? order by ts_lsp desc limit 1"
        );
        scy.query(cql, (series as i64, ts_msp as i64, ts_lsp_max as i64))
            .await
            .err_conv()?
    };
    let mut last_before = None;
    let mut ret = VecDeque::new();
    for row in res.rows_typed_or_empty::<(i64, i32)>() {
        let row = row.err_conv()?;
        let ts = ts_msp + row.0 as u64;
        let kind = row.1 as u32;
        // from netfetch::store::ChannelStatus
        let ev = ConnStatusEvent {
            ts,
            status: ConnStatus::from_ca_ingest_status_kind(kind),
        };
        if ts >= range.end {
        } else if ts >= range.beg {
            ret.push_back(ev);
        } else {
            last_before = Some(ev);
        }
    }
    if do_one_before {
        if let Some(ev) = last_before {
            debug!("PREPENDING THE LAST BEFORE  {ev:?}");
            ret.push_front(ev);
        }
    }
    trace!("found in total {} events  ts_msp {}", ret.len(), ts_msp);
    Ok(ret)
}

struct ReadValues {
    series: u64,
    range: NanoRange,
    ts_msps: VecDeque<u64>,
    fwd: bool,
    do_one_before_range: bool,
    fut: Pin<Box<dyn Future<Output = Result<VecDeque<ConnStatusEvent>, Error>> + Send>>,
    scy: Arc<ScySession>,
}

impl ReadValues {
    fn new(
        series: u64,
        range: NanoRange,
        ts_msps: VecDeque<u64>,
        fwd: bool,
        do_one_before_range: bool,
        scy: Arc<ScySession>,
    ) -> Self {
        let mut ret = Self {
            series,
            range,
            ts_msps,
            fwd,
            do_one_before_range,
            fut: Box::pin(futures_util::future::ready(Err(Error::with_msg_no_trace(
                "future not initialized",
            )))),
            scy,
        };
        ret.next();
        ret
    }

    fn next(&mut self) -> bool {
        if let Some(ts_msp) = self.ts_msps.pop_front() {
            self.fut = self.make_fut(ts_msp);
            true
        } else {
            info!("no more msp");
            false
        }
    }

    fn make_fut(
        &mut self,
        ts_msp: u64,
    ) -> Pin<Box<dyn Future<Output = Result<VecDeque<ConnStatusEvent>, Error>> + Send>> {
        info!("make fut for {ts_msp}");
        let fut = read_next_status_events(
            self.series,
            ts_msp,
            self.range.clone(),
            self.fwd,
            self.do_one_before_range,
            self.scy.clone(),
        );
        Box::pin(fut)
    }
}

enum FrState {
    New,
    ReadValues(ReadValues),
    Done,
}

pub struct StatusStreamScylla {
    state: FrState,
    series: u64,
    range: NanoRange,
    do_one_before_range: bool,
    scy: Arc<ScySession>,
    outbuf: VecDeque<ConnStatusEvent>,
}

impl StatusStreamScylla {
    pub fn new(series: u64, range: NanoRange, do_one_before_range: bool, scy: Arc<ScySession>) -> Self {
        Self {
            state: FrState::New,
            series,
            range,
            do_one_before_range,
            scy,
            outbuf: VecDeque::new(),
        }
    }
}

impl Stream for StatusStreamScylla {
    type Item = Result<ConnStatusEvent, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = tracing::span!(tracing::Level::TRACE, "poll_next");
        let _spg = span.enter();
        loop {
            if let Some(x) = self.outbuf.pop_front() {
                break Ready(Some(Ok(x)));
            }
            break match self.state {
                FrState::New => {
                    let mut ts_msps = VecDeque::new();
                    let mut ts = self.range.beg / CONNECTION_STATUS_DIV * CONNECTION_STATUS_DIV;
                    while ts < self.range.end {
                        info!("Use ts {ts}");
                        ts_msps.push_back(ts);
                        ts += CONNECTION_STATUS_DIV;
                    }
                    let st = ReadValues::new(
                        self.series,
                        self.range.clone(),
                        ts_msps,
                        true,
                        self.do_one_before_range,
                        self.scy.clone(),
                    );
                    self.state = FrState::ReadValues(st);
                    continue;
                }
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        if !st.next() {
                            debug!("ReadValues exhausted");
                            self.state = FrState::Done;
                        }
                        for x in item {
                            self.outbuf.push_back(x);
                        }
                        continue;
                    }
                    Ready(Err(e)) => {
                        error!("{e}");
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::Done => Ready(None),
            };
        }
    }
}
