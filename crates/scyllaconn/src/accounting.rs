use crate::errconv::ErrConv;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::Empty;
use items_0::Extendable;
use items_0::WithLen;
use items_2::accounting::AccountingEvents;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits;
use netpod::EMIT_ACCOUNTING_SNAP;
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

async fn read_next(ts_msp: u64, range: NanoRange, fwd: bool, scy: Arc<ScySession>) -> Result<AccountingEvents, Error> {
    if ts_msp >= range.end {
        warn!(
            "given ts_msp {}  >=  range.end {}  not necessary to read this",
            ts_msp, range.end
        );
    }
    if range.end > i64::MAX as u64 {
        return Err(Error::with_msg_no_trace(format!("range.end overflows i64")));
    }
    let mut ret = AccountingEvents::empty();
    let mut tot_bytes = 0;
    for part in 0..255_u32 {
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
            let cql = concat!("select series, count, bytes from account_00 where part = ? and ts = ?");
            scy.query(cql, (part as i32, ts_msp as i64)).await.err_conv()?
        } else {
            return Err(Error::with_msg_no_trace("no backward support"));
        };
        type RowType = (i64, i64, i64);
        for row in res.rows_typed_or_empty::<RowType>() {
            let row = row.err_conv()?;
            let ts = ts_msp;
            let series = row.0 as u64;
            let count = row.1 as u64;
            let bytes = row.1 as u64;
            tot_bytes += bytes;
        }
    }
    ret.tss.push_back(ts_msp);
    ret.bytes.push_back(tot_bytes);
    trace!("found in total {} events  ts_msp {}", ret.len(), ts_msp);
    Ok(ret)
}

struct ReadValues {
    series: u64,
    range: NanoRange,
    ts_msps: VecDeque<u64>,
    fwd: bool,
    do_one_before_range: bool,
    fut: Pin<Box<dyn Future<Output = Result<AccountingEvents, Error>> + Send>>,
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
            debug!("no more msp");
            false
        }
    }

    fn make_fut(&mut self, ts_msp: u64) -> Pin<Box<dyn Future<Output = Result<AccountingEvents, Error>> + Send>> {
        debug!("make fut for {ts_msp}");
        let fut = read_next(ts_msp, self.range.clone(), self.fwd, self.scy.clone());
        Box::pin(fut)
    }
}

enum FrState {
    New,
    ReadValues(ReadValues),
    Done,
}

pub struct AccountingStreamScylla {
    state: FrState,
    series: u64,
    range: NanoRange,
    scy: Arc<ScySession>,
    outbuf: AccountingEvents,
}

impl AccountingStreamScylla {
    pub fn new(series: u64, range: NanoRange, scy: Arc<ScySession>) -> Self {
        Self {
            state: FrState::New,
            series,
            range,
            scy,
            outbuf: AccountingEvents::empty(),
        }
    }
}

impl Stream for AccountingStreamScylla {
    type Item = Result<AccountingEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = tracing::span!(tracing::Level::TRACE, "poll_next");
        let _spg = span.enter();
        loop {
            if self.outbuf.len() > 0 {
                let item = std::mem::replace(&mut self.outbuf, AccountingEvents::empty());
                break Ready(Some(Ok(item)));
            }
            break match self.state {
                FrState::New => {
                    let mut ts_msps = VecDeque::new();
                    let mut ts = self.range.beg / timeunits::SEC / EMIT_ACCOUNTING_SNAP * EMIT_ACCOUNTING_SNAP;
                    while ts < self.range.end {
                        debug!("use ts {ts}");
                        ts_msps.push_back(ts);
                        ts += EMIT_ACCOUNTING_SNAP;
                    }
                    let fwd = true;
                    let do_one_before_range = false;
                    let st = ReadValues::new(
                        self.series,
                        self.range.clone(),
                        ts_msps,
                        fwd,
                        do_one_before_range,
                        self.scy.clone(),
                    );
                    self.state = FrState::ReadValues(st);
                    continue;
                }
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(mut item)) => {
                        if !st.next() {
                            debug!("ReadValues exhausted");
                            self.state = FrState::Done;
                        }
                        self.outbuf.extend_from(&mut item);
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
