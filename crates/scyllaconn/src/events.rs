use crate::errconv::ErrConv;
use crate::range::ScyllaSeriesRange;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::scalar_ops::ScalarOps;
use items_0::Appendable;
use items_0::Empty;
use items_0::Events;
use items_0::WithLen;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

async fn find_ts_msp(
    series: u64,
    range: ScyllaSeriesRange,
    scy: Arc<ScySession>,
) -> Result<(VecDeque<u64>, VecDeque<u64>), Error> {
    trace!("find_ts_msp  series {}  {:?}", series, range);
    let mut ret1 = VecDeque::new();
    let mut ret2 = VecDeque::new();
    // TODO use prepared statements
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp < ? order by ts_msp desc limit 2";
    let res = scy.query(cql, (series as i64, range.beg() as i64)).await.err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret1.push_front(row.0 as u64);
    }
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp >= ? and ts_msp < ?";
    let res = scy
        .query(cql, (series as i64, range.beg() as i64, range.end() as i64))
        .await
        .err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret2.push_back(row.0 as u64);
    }
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp >= ? limit 1";
    let res = scy.query(cql, (series as i64, range.end() as i64)).await.err_conv()?;
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret2.push_back(row.0 as u64);
    }
    trace!("find_ts_msp  n1 {}  n2 {}", ret1.len(), ret2.len());
    Ok((ret1, ret2))
}

trait ValTy: Sized {
    type ScaTy: ScalarOps + std::default::Default;
    type ScyTy: scylla::cql_to_rust::FromCqlVal<scylla::frame::response::result::CqlValue>;
    type Container: Events + Appendable<Self>;
    fn from_scyty(inp: Self::ScyTy) -> Self;
    fn table_name() -> &'static str;
    fn default() -> Self;
}

macro_rules! impl_scaty_scalar {
    ($st:ty, $st_scy:ty, $table_name:expr) => {
        impl ValTy for $st {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type Container = EventsDim0<Self::ScaTy>;
            fn from_scyty(inp: Self::ScyTy) -> Self {
                inp as Self
            }
            fn table_name() -> &'static str {
                $table_name
            }
            fn default() -> Self {
                <Self as std::default::Default>::default()
            }
        }
    };
}

macro_rules! impl_scaty_array {
    ($vt:ty, $st:ty, $st_scy:ty, $table_name:expr) => {
        impl ValTy for $vt {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type Container = EventsDim1<Self::ScaTy>;
            fn from_scyty(inp: Self::ScyTy) -> Self {
                inp.into_iter().map(|x| x as Self::ScaTy).collect()
            }
            fn table_name() -> &'static str {
                $table_name
            }
            fn default() -> Self {
                Vec::new()
            }
        }
    };
}

impl_scaty_scalar!(u8, i8, "events_scalar_u8");
impl_scaty_scalar!(u16, i16, "events_scalar_u16");
impl_scaty_scalar!(u32, i32, "events_scalar_u32");
impl_scaty_scalar!(u64, i64, "events_scalar_u64");
impl_scaty_scalar!(i8, i8, "events_scalar_i8");
impl_scaty_scalar!(i16, i16, "events_scalar_i16");
impl_scaty_scalar!(i32, i32, "events_scalar_i32");
impl_scaty_scalar!(i64, i64, "events_scalar_i64");
impl_scaty_scalar!(f32, f32, "events_scalar_f32");
impl_scaty_scalar!(f64, f64, "events_scalar_f64");
impl_scaty_scalar!(bool, bool, "events_scalar_bool");
impl_scaty_scalar!(String, String, "events_scalar_string");

impl_scaty_array!(Vec<u8>, u8, Vec<i8>, "events_array_u8");
impl_scaty_array!(Vec<u16>, u16, Vec<i16>, "events_array_u16");
impl_scaty_array!(Vec<u32>, u32, Vec<i32>, "events_array_u32");
impl_scaty_array!(Vec<u64>, u64, Vec<i64>, "events_array_u64");
impl_scaty_array!(Vec<i8>, i8, Vec<i8>, "events_array_i8");
impl_scaty_array!(Vec<i16>, i16, Vec<i16>, "events_array_i16");
impl_scaty_array!(Vec<i32>, i32, Vec<i32>, "events_array_i32");
impl_scaty_array!(Vec<i64>, i64, Vec<i64>, "events_array_i64");
impl_scaty_array!(Vec<f32>, f32, Vec<f32>, "events_array_f32");
impl_scaty_array!(Vec<f64>, f64, Vec<f64>, "events_array_f64");
impl_scaty_array!(Vec<bool>, bool, Vec<bool>, "events_array_bool");
impl_scaty_array!(Vec<String>, String, Vec<String>, "events_array_string");

struct ReadNextValuesOpts {
    series: u64,
    ts_msp: u64,
    range: ScyllaSeriesRange,
    fwd: bool,
    with_values: bool,
    scy: Arc<ScySession>,
}

async fn read_next_values<ST>(opts: ReadNextValuesOpts) -> Result<Box<dyn Events>, Error>
where
    ST: ValTy,
{
    let series = opts.series;
    let ts_msp = opts.ts_msp;
    let range = opts.range;
    let fwd = opts.fwd;
    let scy = opts.scy;
    let table_name = ST::table_name();
    if range.end() > i64::MAX as u64 {
        return Err(Error::with_msg_no_trace(format!("range.end overflows i64")));
    }
    let cql_fields = if opts.with_values {
        "ts_lsp, pulse, value"
    } else {
        "ts_lsp, pulse"
    };
    let ret = if fwd {
        let ts_lsp_min = if ts_msp < range.beg() { range.beg() - ts_msp } else { 0 };
        let ts_lsp_max = if ts_msp < range.end() { range.end() - ts_msp } else { 0 };
        trace!(
            "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  beg {}  end {}  {}",
            ts_msp,
            ts_lsp_min,
            ts_lsp_max,
            range.beg(),
            range.end(),
            table_name,
        );
        // TODO use prepared!
        let cql = format!(
            concat!(
                "select {} from {}",
                " where series = ? and ts_msp = ? and ts_lsp >= ? and ts_lsp < ?"
            ),
            cql_fields, table_name,
        );
        let res = scy
            .query(
                cql,
                (series as i64, ts_msp as i64, ts_lsp_min as i64, ts_lsp_max as i64),
            )
            .await
            .err_conv()?;
        let mut last_before = None;
        let mut ret = ST::Container::empty();
        for row in res.rows().err_conv()? {
            let (ts, pulse, value) = if opts.with_values {
                let row: (i64, i64, ST::ScyTy) = row.into_typed().err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = ValTy::from_scyty(row.2);
                (ts, pulse, value)
            } else {
                let row: (i64, i64) = row.into_typed().err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = ValTy::default();
                (ts, pulse, value)
            };
            if ts >= range.end() {
                // TODO count as logic error
                error!("ts >= range.end");
            } else if ts >= range.beg() {
                if pulse % 27 != 3618 {
                    ret.push(ts, pulse, value);
                }
            } else {
                if last_before.is_none() {
                    warn!("encounter event before range in forward read {ts}");
                }
                last_before = Some((ts, pulse, value));
            }
        }
        ret
    } else {
        let ts_lsp_max = if ts_msp < range.beg() { range.beg() - ts_msp } else { 0 };
        trace!(
            "BCK  ts_msp {}  ts_lsp_max {}  beg {}  end {}  {}",
            ts_msp,
            ts_lsp_max,
            range.beg(),
            range.end(),
            table_name,
        );
        // TODO use prepared!
        let cql = format!(
            concat!(
                "select {} from {}",
                " where series = ? and ts_msp = ? and ts_lsp < ? order by ts_lsp desc limit 1"
            ),
            cql_fields, table_name,
        );
        let res = scy
            .query(cql, (series as i64, ts_msp as i64, ts_lsp_max as i64))
            .await
            .err_conv()?;
        let mut seen_before = false;
        let mut ret = ST::Container::empty();
        for row in res.rows().err_conv()? {
            let (ts, pulse, value) = if opts.with_values {
                let row: (i64, i64, ST::ScyTy) = row.into_typed().err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = ValTy::from_scyty(row.2);
                (ts, pulse, value)
            } else {
                let row: (i64, i64) = row.into_typed().err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = ValTy::default();
                (ts, pulse, value)
            };
            if ts >= range.beg() {
                // TODO count as logic error
                error!("ts >= range.beg");
            } else if ts < range.beg() {
                if pulse % 27 != 3618 {
                    ret.push(ts, pulse, value);
                }
            } else {
                seen_before = true;
            }
        }
        let _ = seen_before;
        if ret.len() > 1 {
            error!("multiple events in backwards search {}", ret.len());
        }
        ret
    };
    trace!("read  ts_msp {}  len {}", ts_msp, ret.len());
    let ret = Box::new(ret);
    Ok(ret)
}

struct ReadValues {
    series: u64,
    scalar_type: ScalarType,
    shape: Shape,
    range: ScyllaSeriesRange,
    ts_msps: VecDeque<u64>,
    fwd: bool,
    with_values: bool,
    fut: Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>,
    fut_done: bool,
    scy: Arc<ScySession>,
}

impl ReadValues {
    fn new(
        series: u64,
        scalar_type: ScalarType,
        shape: Shape,
        range: ScyllaSeriesRange,
        ts_msps: VecDeque<u64>,
        fwd: bool,
        with_values: bool,
        scy: Arc<ScySession>,
    ) -> Self {
        let mut ret = Self {
            series,
            scalar_type,
            shape,
            range,
            ts_msps,
            fwd,
            with_values,
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
        let opts = ReadNextValuesOpts {
            series: self.series.clone(),
            ts_msp,
            range: self.range.clone(),
            fwd: self.fwd,
            with_values: self.with_values,
            scy: self.scy.clone(),
        };
        let scalar_type = self.scalar_type.clone();
        let shape = self.shape.clone();
        let fut = async move {
            match &shape {
                Shape::Scalar => match &scalar_type {
                    ScalarType::U8 => read_next_values::<u8>(opts).await,
                    ScalarType::U16 => read_next_values::<u16>(opts).await,
                    ScalarType::U32 => read_next_values::<u32>(opts).await,
                    ScalarType::U64 => read_next_values::<u64>(opts).await,
                    ScalarType::I8 => read_next_values::<i8>(opts).await,
                    ScalarType::I16 => read_next_values::<i16>(opts).await,
                    ScalarType::I32 => read_next_values::<i32>(opts).await,
                    ScalarType::I64 => read_next_values::<i64>(opts).await,
                    ScalarType::F32 => read_next_values::<f32>(opts).await,
                    ScalarType::F64 => read_next_values::<f64>(opts).await,
                    ScalarType::BOOL => read_next_values::<bool>(opts).await,
                    ScalarType::STRING => read_next_values::<String>(opts).await,
                    _ => {
                        error!("TODO ReadValues add more types");
                        err::todoval()
                    }
                },
                Shape::Wave(_) => match &scalar_type {
                    ScalarType::U8 => read_next_values::<Vec<u8>>(opts).await,
                    ScalarType::U16 => read_next_values::<Vec<u16>>(opts).await,
                    ScalarType::U32 => read_next_values::<Vec<u32>>(opts).await,
                    ScalarType::U64 => read_next_values::<Vec<u64>>(opts).await,
                    ScalarType::I8 => read_next_values::<Vec<i8>>(opts).await,
                    ScalarType::I16 => read_next_values::<Vec<i16>>(opts).await,
                    ScalarType::I32 => read_next_values::<Vec<i32>>(opts).await,
                    ScalarType::I64 => read_next_values::<Vec<i64>>(opts).await,
                    ScalarType::F32 => read_next_values::<Vec<f32>>(opts).await,
                    ScalarType::F64 => read_next_values::<Vec<f64>>(opts).await,
                    ScalarType::BOOL => read_next_values::<Vec<bool>>(opts).await,
                    _ => {
                        error!("TODO ReadValues add more types");
                        err::todoval()
                    }
                },
                _ => {
                    error!("TODO ReadValues add more types");
                    err::todoval()
                }
            }
        };
        Box::pin(fut)
    }
}

enum FrState {
    New,
    FindMsp(Pin<Box<dyn Future<Output = Result<(VecDeque<u64>, VecDeque<u64>), Error>> + Send>>),
    ReadBack1(ReadValues),
    ReadBack2(ReadValues),
    ReadValues(ReadValues),
    DataDone,
    Done,
}

pub struct EventsStreamScylla {
    state: FrState,
    series: u64,
    scalar_type: ScalarType,
    shape: Shape,
    range: ScyllaSeriesRange,
    do_one_before_range: bool,
    ts_msp_bck: VecDeque<u64>,
    ts_msp_fwd: VecDeque<u64>,
    scy: Arc<ScySession>,
    do_test_stream_error: bool,
    found_one_after: bool,
    with_values: bool,
    outqueue: VecDeque<Box<dyn Events>>,
}

impl EventsStreamScylla {
    pub fn new(
        series: u64,
        range: ScyllaSeriesRange,
        do_one_before_range: bool,
        scalar_type: ScalarType,
        shape: Shape,
        with_values: bool,
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
            ts_msp_bck: VecDeque::new(),
            ts_msp_fwd: VecDeque::new(),
            scy,
            do_test_stream_error,
            found_one_after: false,
            with_values,
            outqueue: VecDeque::new(),
        }
    }

    fn ts_msps_found(&mut self, msps1: VecDeque<u64>, msps2: VecDeque<u64>) {
        trace!("ts_msps_found  msps1 {msps1:?}  msps2 {msps2:?}");
        self.ts_msp_bck = msps1;
        self.ts_msp_fwd = msps2;
        for x in self.ts_msp_bck.iter().rev() {
            let x = x.clone();
            if x >= self.range.end() {
                info!("FOUND one-after because of MSP");
                self.found_one_after = true;
            }
            self.ts_msp_fwd.push_front(x);
        }
        trace!("ts_msp_bck {:?}", self.ts_msp_bck);
        trace!("ts_msp_fwd {:?}", self.ts_msp_fwd);
        if let Some(msp) = self.ts_msp_bck.pop_back() {
            trace!("Try ReadBack1");
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                [msp].into(),
                false,
                self.with_values,
                self.scy.clone(),
            );
            self.state = FrState::ReadBack1(st);
        } else if self.ts_msp_fwd.len() > 0 {
            trace!("Go straight for forward read");
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                true,
                self.with_values,
                self.scy.clone(),
            );
            self.state = FrState::ReadValues(st);
        } else {
            self.state = FrState::DataDone;
        }
    }

    fn back_1_done(&mut self, item: Box<dyn Events>) {
        trace!("back_1_done  item len {}", item.len());
        if item.len() > 0 {
            self.outqueue.push_back(item);
            if self.ts_msp_fwd.len() > 0 {
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                    true,
                    self.with_values,
                    self.scy.clone(),
                );
                self.state = FrState::ReadValues(st);
            } else {
                self.state = FrState::DataDone;
            }
        } else {
            if let Some(msp) = self.ts_msp_bck.pop_back() {
                trace!("Try ReadBack2");
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    [msp].into(),
                    false,
                    self.with_values,
                    self.scy.clone(),
                );
                self.state = FrState::ReadBack2(st);
            } else if self.ts_msp_fwd.len() > 0 {
                trace!("No 2nd back MSP, go for forward read");
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                    true,
                    self.with_values,
                    self.scy.clone(),
                );
                self.state = FrState::ReadValues(st);
            } else {
                trace!("No 2nd back MSP, but also nothing to go forward");
                self.state = FrState::DataDone;
            }
        }
    }

    fn back_2_done(&mut self, item: Box<dyn Events>) {
        trace!("back_1_done  item len {}", item.len());
        if item.len() > 0 {
            self.outqueue.push_back(item);
        }
        if self.ts_msp_fwd.len() > 0 {
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                true,
                self.with_values,
                self.scy.clone(),
            );
            self.state = FrState::ReadValues(st);
        } else {
            self.state = FrState::DataDone;
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
                        self.state = FrState::DataDone;
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
                        self.state = FrState::DataDone;
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
                        self.state = FrState::DataDone;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        st.fut_done = true;
                        if !st.next() {
                            trace!("ReadValues exhausted");
                            self.state = FrState::DataDone;
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
                FrState::DataDone => {
                    if self.found_one_after {
                        // TODO emit RangeComplete
                    }
                    self.state = FrState::Done;
                    continue;
                }
                FrState::Done => Ready(None),
            };
        }
    }
}
