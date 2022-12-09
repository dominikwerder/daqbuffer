use crate::errconv::ErrConv;
use err::Error;
use futures_util::{Future, FutureExt, Stream, StreamExt};
use items_0::{Empty, Events, WithLen};
use items_2::channelevents::{ChannelEvents, ConnStatus, ConnStatusEvent};
use items_2::eventsdim0::EventsDim0;
use netpod::log::*;
use netpod::query::{ChannelStateEventsQuery, PlainEventsQuery};
use netpod::timeunits::*;
use netpod::{NanoRange, ScalarType, Shape};
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

async fn find_ts_msp(series: i64, range: NanoRange, scy: Arc<ScySession>) -> Result<VecDeque<u64>, Error> {
    info!("find_ts_msp  series {}  {:?}", series, range);
    let mut ret = VecDeque::new();
    // TODO use prepared statements
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp > ? and ts_msp < ?";
    let res = scy
        .query(cql, (series, range.beg as i64, range.end as i64))
        .await
        .err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret.push_back(row.0 as u64);
    }
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp >= ? limit 1";
    let res = scy.query(cql, (series, range.end as i64)).await.err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret.push_back(row.0 as u64);
    }
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp <= ? order by ts_msp desc limit 2";
    let res = scy.query(cql, (series, range.beg as i64)).await.err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret.push_front(row.0 as u64);
    }
    trace!("found in total {} rows", ret.len());
    Ok(ret)
}

macro_rules! read_next_scalar_values {
    ($fname:ident, $st:ty, $scyty:ty, $table_name:expr) => {
        async fn $fname(
            series: i64,
            ts_msp: u64,
            range: NanoRange,
            fwd: bool,
            do_one_before: bool,
            scy: Arc<ScySession>,
        ) -> Result<EventsDim0<$st>, Error> {
            type ST = $st;
            type SCYTY = $scyty;
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
                    "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  beg {}  end {}  {}",
                    ts_msp,
                    ts_lsp_min,
                    ts_lsp_max,
                    range.beg,
                    range.end,
                    stringify!($fname)
                );
                // TODO use prepared!
                let cql = concat!(
                    "select ts_lsp, pulse, value from ",
                    $table_name,
                    " where series = ? and ts_msp = ? and ts_lsp >= ? and ts_lsp < ?"
                );
                scy.query(cql, (series, ts_msp as i64, ts_lsp_min as i64, ts_lsp_max as i64))
                    .await
                    .err_conv()?
            } else {
                let ts_lsp_max = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
                info!(
                    "BCK  ts_msp {}  ts_lsp_max {}  beg {}  end {}  {}",
                    ts_msp,
                    ts_lsp_max,
                    range.beg,
                    range.end,
                    stringify!($fname)
                );
                // TODO use prepared!
                let cql = concat!(
                    "select ts_lsp, pulse, value from ",
                    $table_name,
                    " where series = ? and ts_msp = ? and ts_lsp < ? order by ts_lsp desc limit 1"
                );
                scy.query(cql, (series, ts_msp as i64, ts_lsp_max as i64))
                    .await
                    .err_conv()?
            };
            let mut last_before = None;
            let mut ret = EventsDim0::<ST>::empty();
            for row in res.rows_typed_or_empty::<(i64, i64, SCYTY)>() {
                let row = row.err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = row.2 as ST;
                if ts >= range.end {
                } else if ts >= range.beg {
                    ret.push(ts, pulse, value);
                } else {
                    last_before = Some((ts, pulse, value));
                }
            }
            if do_one_before {
                if let Some((ts, pulse, value)) = last_before {
                    info!("PREPENDING THE LAST BEFORE  {ts}  {value:?}");
                    ret.push_front(ts, pulse, value);
                }
            }
            info!("found in total {} events  ts_msp {}", ret.tss.len(), ts_msp);
            Ok(ret)
        }
    };
}

macro_rules! read_next_array_values {
    ($fname:ident, $st:ty, $scyty:ty, $table_name:expr) => {
        async fn $fname(
            series: i64,
            ts_msp: u64,
            _range: NanoRange,
            _fwd: bool,
            _do_one_before: bool,
            scy: Arc<ScySession>,
        ) -> Result<EventsDim0<$st>, Error> {
            // TODO change return type: so far EventsDim1 does not exist.
            error!("TODO read_next_array_values");
            err::todo();
            if true {
                return Err(Error::with_msg_no_trace("redo based on scalar case"));
            }
            type ST = $st;
            type _SCYTY = $scyty;
            info!("{}  series {}  ts_msp {}", stringify!($fname), series, ts_msp);
            let cql = concat!(
                "select ts_lsp, pulse, value from ",
                $table_name,
                " where series = ? and ts_msp = ?"
            );
            let _res = scy.query(cql, (series, ts_msp as i64)).await.err_conv()?;
            let ret = EventsDim0::<ST>::empty();
            /*
            for row in res.rows_typed_or_empty::<(i64, i64, Vec<SCYTY>)>() {
                let row = row.err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = row.2.into_iter().map(|x| x as ST).collect();
                ret.push(ts, pulse, value);
            }
            */
            info!("found in total {} events  ts_msp {}", ret.tss.len(), ts_msp);
            Ok(ret)
        }
    };
}

read_next_scalar_values!(read_next_values_scalar_i8, i8, i8, "events_scalar_i8");
read_next_scalar_values!(read_next_values_scalar_i16, i16, i16, "events_scalar_i16");
read_next_scalar_values!(read_next_values_scalar_i32, i32, i32, "events_scalar_i32");
read_next_scalar_values!(read_next_values_scalar_f32, f32, f32, "events_scalar_f32");
read_next_scalar_values!(read_next_values_scalar_f64, f64, f64, "events_scalar_f64");

read_next_array_values!(read_next_values_array_u16, u16, i16, "events_wave_u16");

macro_rules! read_values {
    ($fname:ident, $self:expr, $ts_msp:expr) => {{
        let fut = $fname(
            $self.series,
            $ts_msp,
            $self.range.clone(),
            $self.fwd,
            $self.do_one_before_range,
            $self.scy.clone(),
        );
        let fut = fut.map(|x| match x {
            Ok(k) => {
                let self_name = std::any::type_name::<Self>();
                info!("{self_name} read values len {}", k.len());
                let b = Box::new(k) as Box<dyn Events>;
                Ok(b)
            }
            Err(e) => Err(e),
        });
        let fut = Box::pin(fut) as Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>;
        fut
    }};
}

struct ReadValues {
    series: i64,
    scalar_type: ScalarType,
    shape: Shape,
    range: NanoRange,
    ts_msps: VecDeque<u64>,
    fwd: bool,
    do_one_before_range: bool,
    fut: Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>,
    scy: Arc<ScySession>,
}

impl ReadValues {
    fn new(
        series: i64,
        scalar_type: ScalarType,
        shape: Shape,
        range: NanoRange,
        ts_msps: VecDeque<u64>,
        fwd: bool,
        do_one_before_range: bool,
        scy: Arc<ScySession>,
    ) -> Self {
        let mut ret = Self {
            series,
            scalar_type,
            shape,
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
            self.fut = self.make_fut(ts_msp, self.ts_msps.len() > 0);
            true
        } else {
            false
        }
    }

    fn make_fut(
        &mut self,
        ts_msp: u64,
        _has_more_msp: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>> {
        let fut = match &self.shape {
            Shape::Scalar => match &self.scalar_type {
                ScalarType::I8 => {
                    read_values!(read_next_values_scalar_i8, self, ts_msp)
                }
                ScalarType::I16 => {
                    read_values!(read_next_values_scalar_i16, self, ts_msp)
                }
                ScalarType::I32 => {
                    read_values!(read_next_values_scalar_i32, self, ts_msp)
                }
                ScalarType::F32 => {
                    read_values!(read_next_values_scalar_f32, self, ts_msp)
                }
                ScalarType::F64 => {
                    read_values!(read_next_values_scalar_f64, self, ts_msp)
                }
                _ => {
                    error!("TODO ReadValues add more types");
                    err::todoval()
                }
            },
            Shape::Wave(_) => match &self.scalar_type {
                ScalarType::U16 => {
                    read_values!(read_next_values_array_u16, self, ts_msp)
                }
                _ => {
                    error!("TODO ReadValues add more types");
                    err::todoval()
                }
            },
            _ => {
                error!("TODO ReadValues add more types");
                err::todoval()
            }
        };
        fut
    }
}

enum FrState {
    New,
    FindMsp(Pin<Box<dyn Future<Output = Result<VecDeque<u64>, Error>> + Send>>),
    ReadBack1(ReadValues),
    ReadBack2(ReadValues),
    ReadValues(ReadValues),
    Done,
}

pub struct EventsStreamScylla {
    state: FrState,
    series: u64,
    scalar_type: ScalarType,
    shape: Shape,
    range: NanoRange,
    do_one_before_range: bool,
    ts_msps: VecDeque<u64>,
    scy: Arc<ScySession>,
    do_test_stream_error: bool,
}

impl EventsStreamScylla {
    pub fn new(
        series: u64,
        range: NanoRange,
        do_one_before_range: bool,
        scalar_type: ScalarType,
        shape: Shape,
        scy: Arc<ScySession>,
        do_test_stream_error: bool,
    ) -> Self {
        let self_name = std::any::type_name::<Self>();
        info!("{self_name}  do_one_before_range {do_one_before_range}");
        Self {
            state: FrState::New,
            series,
            scalar_type,
            shape,
            range,
            do_one_before_range,
            ts_msps: VecDeque::new(),
            scy,
            do_test_stream_error,
        }
    }

    fn ts_msps_found(&mut self, ts_msps: VecDeque<u64>) {
        info!("ts_msps_found  ts_msps {ts_msps:?}");
        self.ts_msps = ts_msps;
        // Find the largest MSP which can potentially contain some event before the range.
        let befores: Vec<_> = self
            .ts_msps
            .iter()
            .map(|x| *x)
            .filter(|x| *x < self.range.beg)
            .collect();
        if self.do_one_before_range && befores.len() >= 1 {
            info!("Try ReadBack1");
            let st = ReadValues::new(
                self.series as i64,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                [befores[befores.len() - 1]].into(),
                false,
                self.do_one_before_range,
                self.scy.clone(),
            );
            self.state = FrState::ReadBack1(st);
        } else if self.ts_msps.len() >= 1 {
            info!("Go straight for forward read");
            let st = ReadValues::new(
                self.series as i64,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                self.ts_msps.clone(),
                true,
                self.do_one_before_range,
                self.scy.clone(),
            );
            self.state = FrState::ReadValues(st);
        } else {
            self.state = FrState::Done;
        }
    }

    fn back_1_done(&mut self, item: Box<dyn Events>) -> Option<Box<dyn Events>> {
        info!("back_1_done  len {}", item.len());
        if item.len() == 0 {
            info!("ReadBack1 returned empty");
            // Find the 2nd largest MSP which can potentially contain some event before the range.
            let befores: Vec<_> = self
                .ts_msps
                .iter()
                .map(|x| *x)
                .filter(|x| *x < self.range.beg)
                .collect();
            if befores.len() >= 2 {
                info!("Try ReadBack2");
                let st = ReadValues::new(
                    self.series as i64,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    [befores[befores.len() - 2]].into(),
                    false,
                    self.do_one_before_range,
                    self.scy.clone(),
                );
                self.state = FrState::ReadBack2(st);
                None
            } else if self.ts_msps.len() >= 1 {
                info!("No 2nd back MSP, go for forward read");
                let st = ReadValues::new(
                    self.series as i64,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    self.ts_msps.clone(),
                    true,
                    self.do_one_before_range,
                    self.scy.clone(),
                );
                self.state = FrState::ReadValues(st);
                None
            } else {
                self.state = FrState::Done;
                None
            }
        } else {
            info!("FOUND ONE BEFORE");
            if self.ts_msps.len() > 0 {
                let st = ReadValues::new(
                    self.series as i64,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    self.ts_msps.clone(),
                    true,
                    self.do_one_before_range,
                    self.scy.clone(),
                );
                self.state = FrState::ReadValues(st);
                Some(item)
            } else {
                self.state = FrState::Done;
                Some(item)
            }
        }
    }

    fn back_2_done(&mut self, item: Box<dyn Events>) -> Option<Box<dyn Events>> {
        info!("back_2_done  len {}", item.len());
        if item.len() == 0 {
            info!("ReadBack2 returned empty");
        }
        if self.ts_msps.len() >= 1 {
            let st = ReadValues::new(
                self.series as i64,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                self.ts_msps.clone(),
                true,
                self.do_one_before_range,
                self.scy.clone(),
            );
            self.state = FrState::ReadValues(st);
        } else {
            self.state = FrState::Done;
        }
        if item.len() > 0 {
            Some(item)
        } else {
            None
        }
    }
}

impl Stream for EventsStreamScylla {
    type Item = Result<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.do_test_stream_error {
            let e = Error::with_msg(format!("Test PRIVATE STREAM error."))
                .add_public_msg(format!("Test PUBLIC STREAM error."));
            return Ready(Some(Err(e)));
        }
        loop {
            break match self.state {
                FrState::New => {
                    let fut = find_ts_msp(self.series as i64, self.range.clone(), self.scy.clone());
                    let fut = Box::pin(fut);
                    self.state = FrState::FindMsp(fut);
                    continue;
                }
                FrState::FindMsp(ref mut fut) => match fut.poll_unpin(cx) {
                    Ready(Ok(ts_msps)) => {
                        self.ts_msps_found(ts_msps);
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.state = FrState::Done;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::ReadBack1(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        if let Some(item) = self.back_1_done(item) {
                            item.verify();
                            item.output_info();
                            Ready(Some(Ok(ChannelEvents::Events(item))))
                        } else {
                            continue;
                        }
                    }
                    Ready(Err(e)) => {
                        self.state = FrState::Done;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::ReadBack2(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        if let Some(item) = self.back_2_done(item) {
                            item.verify();
                            item.output_info();
                            Ready(Some(Ok(ChannelEvents::Events(item))))
                        } else {
                            continue;
                        }
                    }
                    Ready(Err(e)) => {
                        self.state = FrState::Done;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        item.verify();
                        item.output_info();
                        if !st.next() {
                            info!("ReadValues exhausted");
                            self.state = FrState::Done;
                        }
                        Ready(Some(Ok(ChannelEvents::Events(item))))
                    }
                    Ready(Err(e)) => Ready(Some(Err(e))),
                    Pending => Pending,
                },
                FrState::Done => Ready(None),
            };
        }
    }
}

pub async fn make_scylla_stream(
    evq: &PlainEventsQuery,
    do_one_before_range: bool,
    series: u64,
    scalar_type: ScalarType,
    shape: Shape,
    scy: Arc<ScySession>,
    do_test_stream_error: bool,
) -> Result<EventsStreamScylla, Error> {
    let res = EventsStreamScylla::new(
        series,
        evq.range().clone(),
        do_one_before_range,
        scalar_type,
        shape,
        scy,
        do_test_stream_error,
    );
    Ok(res)
}

pub async fn channel_state_events(
    evq: &ChannelStateEventsQuery,
    scy: Arc<ScySession>,
) -> Result<Pin<Box<dyn Stream<Item = Result<ChannelEvents, Error>> + Send>>, Error> {
    let (tx, rx) = async_channel::bounded(8);
    let evq = evq.clone();
    let fut = async move {
        let div = DAY;
        let mut ts_msp = evq.range().beg / div * div;
        loop {
            let series = (evq
                .channel()
                .series()
                .ok_or(Error::with_msg_no_trace(format!("series id not given"))))?;
            let params = (series as i64, ts_msp as i64);
            let mut res = scy
                .query_iter(
                    "select ts_lsp, kind from channel_status where series = ? and ts_msp = ?",
                    params,
                )
                .await
                .err_conv()?;
            while let Some(row) = res.next().await {
                let row = row.err_conv()?;
                let (ts_lsp, kind): (i64, i32) = row.into_typed().err_conv()?;
                let ts = ts_msp + ts_lsp as u64;
                let kind = kind as u32;
                if ts >= evq.range().beg && ts < evq.range().end {
                    let status = match kind {
                        1 => ConnStatus::Connect,
                        2 => ConnStatus::Disconnect,
                        _ => {
                            let e = Error::with_msg_no_trace(format!("bad status kind {kind}"));
                            let e2 = Error::with_msg_no_trace(format!("bad status kind {kind}"));
                            let _ = tx.send(Err(e)).await;
                            return Err(e2);
                        }
                    };
                    let ev = ConnStatusEvent { ts, status };
                    tx.send(Ok(ChannelEvents::Status(ev)))
                        .await
                        .map_err(|e| format!("{e}"))?;
                }
            }
            ts_msp += div;
            if ts_msp >= evq.range().end {
                break;
            }
        }
        Ok(())
    };
    // TODO join the task (better: rewrite as proper stream)
    tokio::spawn(fut);
    Ok(Box::pin(rx))
}
