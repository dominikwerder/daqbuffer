use crate::errconv::ErrConv;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::Empty;
use items_0::Events;
use items_0::WithLen;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use netpod::log::*;
use netpod::NanoRange;
use netpod::ScalarType;
use netpod::Shape;
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

async fn find_ts_msp(
    series: u64,
    range: NanoRange,
    scy: Arc<ScySession>,
) -> Result<(VecDeque<u64>, VecDeque<u64>), Error> {
    trace!("find_ts_msp  series {}  {:?}", series, range);
    let mut ret1 = VecDeque::new();
    let mut ret2 = VecDeque::new();
    // TODO use prepared statements
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp <= ? order by ts_msp desc limit 2";
    let res = scy.query(cql, (series as i64, range.beg as i64)).await.err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret1.push_front(row.0 as u64);
    }
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp > ? and ts_msp < ?";
    let res = scy
        .query(cql, (series as i64, range.beg as i64, range.end as i64))
        .await
        .err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret2.push_back(row.0 as u64);
    }
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp >= ? limit 1";
    let res = scy.query(cql, (series as i64, range.end as i64)).await.err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret2.push_back(row.0 as u64);
    }
    trace!("find_ts_msp  n1 {}  n2 {}", ret1.len(), ret2.len());
    Ok((ret1, ret2))
}

macro_rules! read_next_scalar_values {
    ($fname:ident, $st:ty, $scyty:ty, $table_name:expr) => {
        async fn $fname(
            series: u64,
            ts_msp: u64,
            range: NanoRange,
            fwd: bool,
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
            let ret = if fwd {
                let ts_lsp_min = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
                let ts_lsp_max = if ts_msp < range.end { range.end - ts_msp } else { 0 };
                trace!(
                    "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  beg {}  end {}  {}",
                    ts_msp,
                    ts_lsp_min,
                    ts_lsp_max,
                    range.beg,
                    range.end,
                    stringify!($table_name)
                );
                // TODO use prepared!
                let cql = concat!(
                    "select ts_lsp, pulse, value from ",
                    $table_name,
                    " where series = ? and ts_msp = ? and ts_lsp >= ? and ts_lsp < ?"
                );
                let res = scy
                    .query(
                        cql,
                        (series as i64, ts_msp as i64, ts_lsp_min as i64, ts_lsp_max as i64),
                    )
                    .await
                    .err_conv()?;
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
                        if last_before.is_none() {
                            warn!("encounter event before range in forward read {ts}");
                        }
                        last_before = Some((ts, pulse, value));
                    }
                }
                ret
            } else {
                let ts_lsp_max = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
                trace!(
                    "BCK  ts_msp {}  ts_lsp_max {}  beg {}  end {}  {}",
                    ts_msp,
                    ts_lsp_max,
                    range.beg,
                    range.end,
                    stringify!($table_name)
                );
                // TODO use prepared!
                let cql = concat!(
                    "select ts_lsp, pulse, value from ",
                    $table_name,
                    " where series = ? and ts_msp = ? and ts_lsp < ? order by ts_lsp desc limit 1"
                );
                let res = scy
                    .query(cql, (series as i64, ts_msp as i64, ts_lsp_max as i64))
                    .await
                    .err_conv()?;
                let mut seen_before = false;
                let mut ret = EventsDim0::<ST>::empty();
                for row in res.rows_typed_or_empty::<(i64, i64, SCYTY)>() {
                    let row = row.err_conv()?;
                    let ts = ts_msp + row.0 as u64;
                    let pulse = row.1 as u64;
                    let value = row.2 as ST;
                    if ts >= range.end {
                    } else if ts < range.beg {
                        ret.push(ts, pulse, value);
                    } else {
                        if !seen_before {
                            warn!("encounter event before range in forward read {ts}");
                        }
                        seen_before = true;
                    }
                }
                ret
            };
            trace!("found in total {} events  ts_msp {}", ret.tss.len(), ts_msp);
            Ok(ret)
        }
    };
}

macro_rules! read_next_array_values {
    ($fname:ident, $st:ty, $scyty:ty, $table_name:expr) => {
        async fn $fname(
            series: u64,
            ts_msp: u64,
            range: NanoRange,
            fwd: bool,
            scy: Arc<ScySession>,
        ) -> Result<EventsDim1<$st>, Error> {
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
            let ret = if fwd {
                let ts_lsp_min = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
                let ts_lsp_max = if ts_msp < range.end { range.end - ts_msp } else { 0 };
                trace!(
                    "FWD  {}  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  beg {}  end {}  {}",
                    stringify!($fname),
                    ts_msp,
                    ts_lsp_min,
                    ts_lsp_max,
                    range.beg,
                    range.end,
                    stringify!($table_name)
                );
                // TODO use prepared!
                let cql = concat!(
                    "select ts_lsp, pulse, value from ",
                    $table_name,
                    " where series = ? and ts_msp = ? and ts_lsp >= ? and ts_lsp < ?"
                );
                let res = scy
                    .query(
                        cql,
                        (series as i64, ts_msp as i64, ts_lsp_min as i64, ts_lsp_max as i64),
                    )
                    .await
                    .err_conv()?;
                let mut last_before = None;
                let mut ret = EventsDim1::<ST>::empty();
                for row in res.rows_typed_or_empty::<(i64, i64, Vec<SCYTY>)>() {
                    let row = row.err_conv()?;
                    let ts = ts_msp + row.0 as u64;
                    let pulse = row.1 as u64;
                    let value = row.2.into_iter().map(|x| x as ST).collect();
                    if ts >= range.end {
                    } else if ts >= range.beg {
                        ret.push(ts, pulse, value);
                    } else {
                        if last_before.is_none() {
                            warn!("encounter event before range in forward read {ts}");
                        }
                        last_before = Some((ts, pulse, value));
                    }
                }
                ret
            } else {
                let ts_lsp_max = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
                trace!(
                    "BCK  {}  ts_msp {}  ts_lsp_max {}  beg {}  end {}  {}",
                    stringify!($fname),
                    ts_msp,
                    ts_lsp_max,
                    range.beg,
                    range.end,
                    stringify!($table_name)
                );
                // TODO use prepared!
                let cql = concat!(
                    "select ts_lsp, pulse, value from ",
                    $table_name,
                    " where series = ? and ts_msp = ? and ts_lsp < ? order by ts_lsp desc limit 1"
                );
                let res = scy
                    .query(cql, (series as i64, ts_msp as i64, ts_lsp_max as i64))
                    .await
                    .err_conv()?;
                let mut seen_before = false;
                let mut ret = EventsDim1::<ST>::empty();
                for row in res.rows_typed_or_empty::<(i64, i64, Vec<SCYTY>)>() {
                    let row = row.err_conv()?;
                    let ts = ts_msp + row.0 as u64;
                    let pulse = row.1 as u64;
                    let value = row.2.into_iter().map(|x| x as ST).collect();
                    if ts >= range.end {
                    } else if ts < range.beg {
                        ret.push(ts, pulse, value);
                    } else {
                        if !seen_before {
                            warn!("encounter event before range in forward read {ts}");
                        }
                        seen_before = true;
                    }
                }
                ret
            };
            trace!("found in total {} events  ts_msp {}", ret.tss.len(), ts_msp);
            Ok(ret)
        }
    };
}

read_next_scalar_values!(read_next_values_scalar_u8, u8, i8, "events_scalar_u8");
read_next_scalar_values!(read_next_values_scalar_u16, u16, i16, "events_scalar_u16");
read_next_scalar_values!(read_next_values_scalar_u32, u32, i32, "events_scalar_u32");
read_next_scalar_values!(read_next_values_scalar_u64, u64, i64, "events_scalar_u64");
read_next_scalar_values!(read_next_values_scalar_i8, i8, i8, "events_scalar_i8");
read_next_scalar_values!(read_next_values_scalar_i16, i16, i16, "events_scalar_i16");
read_next_scalar_values!(read_next_values_scalar_i32, i32, i32, "events_scalar_i32");
read_next_scalar_values!(read_next_values_scalar_i64, i64, i64, "events_scalar_i64");
read_next_scalar_values!(read_next_values_scalar_f32, f32, f32, "events_scalar_f32");
read_next_scalar_values!(read_next_values_scalar_f64, f64, f64, "events_scalar_f64");

read_next_array_values!(read_next_values_array_u16, u16, i16, "events_array_u16");
read_next_array_values!(read_next_values_array_bool, bool, bool, "events_array_bool");

macro_rules! read_values {
    ($fname:ident, $self:expr, $ts_msp:expr) => {{
        let fut = $fname($self.series, $ts_msp, $self.range.clone(), $self.fwd, $self.scy.clone());
        let fut = fut.map(|x| match x {
            Ok(k) => {
                let self_name = std::any::type_name::<Self>();
                trace!("{self_name} read values len {}", k.len());
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
    series: u64,
    scalar_type: ScalarType,
    shape: Shape,
    range: NanoRange,
    ts_msps: VecDeque<u64>,
    fwd: bool,
    fut: Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>,
    fut_done: bool,
    scy: Arc<ScySession>,
}

impl ReadValues {
    fn new(
        series: u64,
        scalar_type: ScalarType,
        shape: Shape,
        range: NanoRange,
        ts_msps: VecDeque<u64>,
        fwd: bool,
        scy: Arc<ScySession>,
    ) -> Self {
        let mut ret = Self {
            series,
            scalar_type,
            shape,
            range,
            ts_msps,
            fwd,
            fut: Box::pin(futures_util::future::ready(Err(Error::with_msg_no_trace(
                "future not initialized",
            )))),
            fut_done: false,
            scy,
        };
        ret.next();
        ret
    }

    fn next(&mut self) -> bool {
        if let Some(ts_msp) = self.ts_msps.pop_front() {
            self.fut = self.make_fut(ts_msp);
            self.fut_done = false;
            true
        } else {
            false
        }
    }

    fn make_fut(&mut self, ts_msp: u64) -> Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>> {
        let fut = match &self.shape {
            Shape::Scalar => match &self.scalar_type {
                ScalarType::U8 => {
                    read_values!(read_next_values_scalar_u8, self, ts_msp)
                }
                ScalarType::U16 => {
                    read_values!(read_next_values_scalar_u16, self, ts_msp)
                }
                ScalarType::U32 => {
                    read_values!(read_next_values_scalar_u32, self, ts_msp)
                }
                ScalarType::U64 => {
                    read_values!(read_next_values_scalar_u64, self, ts_msp)
                }
                ScalarType::I8 => {
                    read_values!(read_next_values_scalar_i8, self, ts_msp)
                }
                ScalarType::I16 => {
                    read_values!(read_next_values_scalar_i16, self, ts_msp)
                }
                ScalarType::I32 => {
                    read_values!(read_next_values_scalar_i32, self, ts_msp)
                }
                ScalarType::I64 => {
                    read_values!(read_next_values_scalar_i64, self, ts_msp)
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
                ScalarType::BOOL => {
                    info!("attempt to read bool");
                    read_values!(read_next_values_array_bool, self, ts_msp)
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
    FindMsp(Pin<Box<dyn Future<Output = Result<(VecDeque<u64>, VecDeque<u64>), Error>> + Send>>),
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
    #[allow(unused)]
    do_one_before_range: bool,
    ts_msp_b1: Option<u64>,
    ts_msp_b2: Option<u64>,
    ts_msps: VecDeque<u64>,
    scy: Arc<ScySession>,
    do_test_stream_error: bool,
    outqueue: VecDeque<Box<dyn Events>>,
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
        Self {
            state: FrState::New,
            series,
            scalar_type,
            shape,
            range,
            do_one_before_range,
            ts_msp_b1: None,
            ts_msp_b2: None,
            ts_msps: VecDeque::new(),
            scy,
            do_test_stream_error,
            outqueue: VecDeque::new(),
        }
    }

    fn ts_msps_found(&mut self, msps1: VecDeque<u64>, msps2: VecDeque<u64>) {
        trace!("ts_msps_found  msps1 {msps1:?}  msps2 {msps2:?}");
        let mut msps1 = msps1;
        self.ts_msp_b1 = msps1.pop_back();
        self.ts_msp_b2 = msps1.pop_back();
        self.ts_msps = msps2;
        if let Some(x) = self.ts_msp_b1.clone() {
            self.ts_msps.push_front(x);
        }
        trace!("ts_msp_b1 {:?}", self.ts_msp_b1);
        trace!("ts_msp_b2 {:?}", self.ts_msp_b2);
        trace!("ts_msps {:?}", self.ts_msps);
        if let Some(msp) = self.ts_msp_b1.clone() {
            trace!("Try ReadBack1");
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                [msp].into(),
                false,
                self.scy.clone(),
            );
            self.state = FrState::ReadBack1(st);
        } else if self.ts_msps.len() >= 1 {
            trace!("Go straight for forward read");
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                self.ts_msps.clone(),
                true,
                self.scy.clone(),
            );
            self.state = FrState::ReadValues(st);
        } else {
            self.state = FrState::Done;
        }
    }

    fn back_1_done(&mut self, item: Box<dyn Events>) {
        trace!("back_1_done  item len {}", item.len());
        if item.len() > 0 {
            self.outqueue.push_back(item);
            if self.ts_msps.len() > 0 {
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    self.ts_msps.clone(),
                    true,
                    self.scy.clone(),
                );
                self.state = FrState::ReadValues(st);
            } else {
                self.state = FrState::Done;
            }
        } else {
            if let Some(msp) = self.ts_msp_b2.clone() {
                trace!("Try ReadBack2");
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    [msp].into(),
                    false,
                    self.scy.clone(),
                );
                self.state = FrState::ReadBack2(st);
            } else if self.ts_msps.len() >= 1 {
                trace!("No 2nd back MSP, go for forward read");
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    self.ts_msps.clone(),
                    true,
                    self.scy.clone(),
                );
                self.state = FrState::ReadValues(st);
            } else {
                self.state = FrState::Done;
            }
        }
    }

    fn back_2_done(&mut self, item: Box<dyn Events>) {
        trace!("back_1_done  item len {}", item.len());
        if item.len() > 0 {
            self.outqueue.push_back(item);
        }
        if self.ts_msps.len() >= 1 {
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                self.ts_msps.clone(),
                true,
                self.scy.clone(),
            );
            self.state = FrState::ReadValues(st);
        } else {
            self.state = FrState::Done;
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
            if let Some(item) = self.outqueue.pop_front() {
                item.verify();
                item.output_info();
                break Ready(Some(Ok(ChannelEvents::Events(item))));
            }
            break match self.state {
                FrState::New => {
                    let fut = find_ts_msp(self.series, self.range.clone(), self.scy.clone());
                    let fut = Box::pin(fut);
                    self.state = FrState::FindMsp(fut);
                    continue;
                }
                FrState::FindMsp(ref mut fut) => match fut.poll_unpin(cx) {
                    Ready(Ok((msps1, msps2))) => {
                        self.ts_msps_found(msps1, msps2);
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
                        st.fut_done = true;
                        self.back_1_done(item);
                        continue;
                    }
                    Ready(Err(e)) => {
                        st.fut_done = true;
                        self.state = FrState::Done;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::ReadBack2(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        st.fut_done = true;
                        self.back_2_done(item);
                        continue;
                    }
                    Ready(Err(e)) => {
                        st.fut_done = true;
                        self.state = FrState::Done;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        st.fut_done = true;
                        if !st.next() {
                            trace!("ReadValues exhausted");
                            self.state = FrState::Done;
                        }
                        if item.len() > 0 {
                            self.outqueue.push_back(item);
                        }
                        continue;
                    }
                    Ready(Err(e)) => {
                        st.fut_done = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::Done => Ready(None),
            };
        }
    }
}
