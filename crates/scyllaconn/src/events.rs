use crate::events2::events::EventReadOpts;
use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use core::fmt;
use err::thiserror;
use err::ThisError;
use futures_util::Future;
use futures_util::StreamExt;
use items_0::scalar_ops::ScalarOps;
use items_0::Appendable;
use items_0::Empty;
use items_0::Events;
use items_0::WithLen;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use netpod::log::*;
use netpod::ttl::RetentionTime;
use netpod::DtNano;
use netpod::EnumVariant;
use netpod::TsMs;
use netpod::TsNano;
use scylla::frame::response::result::Row;
use scylla::Session;
use series::SeriesId;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

macro_rules! trace_fetch { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

#[derive(Debug, ThisError)]
#[cstm(name = "ScyllaReadEvents")]
pub enum Error {
    Prepare(#[from] crate::events2::prepare::Error),
    ScyllaQuery(#[from] scylla::transport::errors::QueryError),
    ScyllaNextRow(#[from] scylla::transport::iterator::NextRowError),
    ScyllaTypeConv(#[from] scylla::cql_to_rust::FromRowError),
    ScyllaWorker(Box<crate::worker::Error>),
    MissingQuery(String),
    NotTokenAware,
    RangeEndOverflow,
    InvalidFuture,
    TestError(String),
    Logic,
    TodoUnsupported,
}

impl From<crate::worker::Error> for Error {
    fn from(value: crate::worker::Error) -> Self {
        Self::ScyllaWorker(Box::new(value))
    }
}

pub(super) trait ValTy: Sized + 'static {
    type ScaTy: ScalarOps + std::default::Default;
    type ScyTy: scylla::cql_to_rust::FromCqlVal<scylla::frame::response::result::CqlValue>;
    type Container: Events + Appendable<Self>;
    fn from_scyty(inp: Self::ScyTy) -> Self;
    fn from_valueblob(inp: Vec<u8>) -> Self;
    fn table_name() -> &'static str;
    fn default() -> Self;
    fn is_valueblob() -> bool;
    fn st_name() -> &'static str;
    fn read_next_values_trait(
        opts: ReadNextValuesOpts,
        jobtrace: ReadJobTrace,
        scy: Arc<Session>,
        stmts: Arc<StmtsEvents>,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn Events>, ReadJobTrace), Error>> + Send>>;
    fn convert_rows(
        rows: Vec<Row>,
        range: ScyllaSeriesRange,
        ts_msp: TsMs,
        with_values: bool,
        bck: bool,
        last_before: &mut Option<(TsNano, Self)>,
    ) -> Result<Self::Container, Error>;
}

macro_rules! impl_scaty_scalar {
    ($st:ty, $st_scy:ty, $st_name:expr, $table_name:expr) => {
        impl ValTy for $st {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type Container = EventsDim0<Self::ScaTy>;

            fn from_scyty(inp: Self::ScyTy) -> Self {
                inp as Self
            }

            fn from_valueblob(_inp: Vec<u8>) -> Self {
                <Self as ValTy>::default()
            }

            fn table_name() -> &'static str {
                concat!("scalar_", $table_name)
            }

            fn default() -> Self {
                <Self as std::default::Default>::default()
            }

            fn is_valueblob() -> bool {
                false
            }

            fn st_name() -> &'static str {
                $st_name
            }

            fn read_next_values_trait(
                opts: ReadNextValuesOpts,
                jobtrace: ReadJobTrace,
                scy: Arc<Session>,
                stmts: Arc<StmtsEvents>,
            ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn Events>, ReadJobTrace), Error>> + Send>> {
                Box::pin(read_next_values_2::<Self>(opts, jobtrace, scy, stmts))
            }

            fn convert_rows(
                rows: Vec<Row>,
                range: ScyllaSeriesRange,
                ts_msp: TsMs,
                with_values: bool,
                bck: bool,
                last_before: &mut Option<(TsNano, Self)>,
            ) -> Result<Self::Container, Error> {
                convert_rows_0::<Self>(rows, range, ts_msp, with_values, bck, last_before)
            }
        }
    };
}

macro_rules! impl_scaty_array {
    ($vt:ty, $st:ty, $st_scy:ty, $st_name:expr, $table_name:expr) => {
        impl ValTy for $vt {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type Container = EventsDim1<Self::ScaTy>;

            fn from_scyty(inp: Self::ScyTy) -> Self {
                inp.into_iter().map(|x| x as Self::ScaTy).collect()
            }

            fn from_valueblob(inp: Vec<u8>) -> Self {
                if inp.len() < 32 {
                    <Self as ValTy>::default()
                } else {
                    let en = std::mem::size_of::<Self::ScaTy>();
                    let n = (inp.len().max(32) - 32) / en;
                    let mut c = Vec::with_capacity(n);
                    for i in 0..n {
                        let r1 = &inp[32 + en * (0 + i)..32 + en * (1 + i)];
                        let p1 = r1 as *const _ as *const $st;
                        let v1 = unsafe { p1.read_unaligned() };
                        c.push(v1);
                    }
                    c
                }
            }

            fn table_name() -> &'static str {
                concat!("array_", $table_name)
            }

            fn default() -> Self {
                Vec::new()
            }

            fn is_valueblob() -> bool {
                true
            }

            fn st_name() -> &'static str {
                $st_name
            }

            fn read_next_values_trait(
                opts: ReadNextValuesOpts,
                jobtrace: ReadJobTrace,
                scy: Arc<Session>,
                stmts: Arc<StmtsEvents>,
            ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn Events>, ReadJobTrace), Error>> + Send>> {
                Box::pin(read_next_values_2::<Self>(opts, jobtrace, scy, stmts))
            }

            fn convert_rows(
                rows: Vec<Row>,
                range: ScyllaSeriesRange,
                ts_msp: TsMs,
                with_values: bool,
                bck: bool,
                last_before: &mut Option<(TsNano, Self)>,
            ) -> Result<Self::Container, Error> {
                convert_rows_0::<Self>(rows, range, ts_msp, with_values, bck, last_before)
            }
        }
    };
}

impl ValTy for EnumVariant {
    type ScaTy = EnumVariant;
    type ScyTy = i16;
    type Container = EventsDim0<EnumVariant>;

    fn from_scyty(inp: Self::ScyTy) -> Self {
        let _ = inp;
        panic!("uses more specialized impl")
    }

    fn from_valueblob(inp: Vec<u8>) -> Self {
        let _ = inp;
        panic!("uses more specialized impl")
    }

    fn table_name() -> &'static str {
        "array_string"
    }

    fn default() -> Self {
        <Self as Default>::default()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "enum"
    }

    fn read_next_values_trait(
        opts: ReadNextValuesOpts,
        jobtrace: ReadJobTrace,
        scy: Arc<Session>,
        stmts: Arc<StmtsEvents>,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn Events>, ReadJobTrace), Error>> + Send>> {
        Box::pin(read_next_values_2::<Self>(opts, jobtrace, scy, stmts))
    }

    fn convert_rows(
        rows: Vec<Row>,
        range: ScyllaSeriesRange,
        ts_msp: TsMs,
        with_values: bool,
        bck: bool,
        last_before: &mut Option<(TsNano, Self)>,
    ) -> Result<Self::Container, Error> {
        convert_rows_enum(rows, range, ts_msp, with_values, bck, last_before)
    }
}

impl ValTy for Vec<String> {
    type ScaTy = String;
    type ScyTy = Vec<String>;
    type Container = EventsDim1<String>;

    fn from_scyty(inp: Self::ScyTy) -> Self {
        inp
    }

    fn from_valueblob(inp: Vec<u8>) -> Self {
        let _ = inp;
        warn!("ValTy::from_valueblob for Vec<String>");
        Vec::new()
    }

    fn table_name() -> &'static str {
        "array_string"
    }

    fn default() -> Self {
        Vec::new()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "string"
    }

    fn read_next_values_trait(
        opts: ReadNextValuesOpts,
        jobtrace: ReadJobTrace,
        scy: Arc<Session>,
        stmts: Arc<StmtsEvents>,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn Events>, ReadJobTrace), Error>> + Send>> {
        let fut = read_next_values_2::<Self>(opts, jobtrace, scy, stmts);
        Box::pin(fut)
    }

    fn convert_rows(
        rows: Vec<Row>,
        range: ScyllaSeriesRange,
        ts_msp: TsMs,
        with_values: bool,
        bck: bool,
        last_before: &mut Option<(TsNano, Self)>,
    ) -> Result<Self::Container, Error> {
        convert_rows_0::<Self>(rows, range, ts_msp, with_values, bck, last_before)
    }
}

impl_scaty_scalar!(u8, i8, "u8", "u8");
impl_scaty_scalar!(u16, i16, "u16", "u16");
impl_scaty_scalar!(u32, i32, "u32", "u32");
impl_scaty_scalar!(u64, i64, "u64", "u64");
impl_scaty_scalar!(i8, i8, "i8", "i8");
impl_scaty_scalar!(i16, i16, "i16", "i16");
impl_scaty_scalar!(i32, i32, "i32", "i32");
impl_scaty_scalar!(i64, i64, "i64", "i64");
impl_scaty_scalar!(f32, f32, "f32", "f32");
impl_scaty_scalar!(f64, f64, "f64", "f64");
impl_scaty_scalar!(bool, bool, "bool", "bool");
impl_scaty_scalar!(String, String, "string", "string");

impl_scaty_array!(Vec<u8>, u8, Vec<i8>, "u8", "u8");
impl_scaty_array!(Vec<u16>, u16, Vec<i16>, "u16", "u16");
impl_scaty_array!(Vec<u32>, u32, Vec<i32>, "u32", "u32");
impl_scaty_array!(Vec<u64>, u64, Vec<i64>, "u64", "u64");
impl_scaty_array!(Vec<i8>, i8, Vec<i8>, "i8", "i8");
impl_scaty_array!(Vec<i16>, i16, Vec<i16>, "i16", "i16");
impl_scaty_array!(Vec<i32>, i32, Vec<i32>, "i32", "i32");
impl_scaty_array!(Vec<i64>, i64, Vec<i64>, "i64", "i64");
impl_scaty_array!(Vec<f32>, f32, Vec<f32>, "f32", "f32");
impl_scaty_array!(Vec<f64>, f64, Vec<f64>, "f64", "f64");
impl_scaty_array!(Vec<bool>, bool, Vec<bool>, "bool", "bool");

#[derive(Debug)]
pub enum ReadEventKind {
    Create,
    FutgenCallingReadNextValues,
    FutgenFutureCreated,
    CallExecuteIter,
    ScyllaReadRow(u32),
    ScyllaReadRowDone(u32),
    ReadNextValuesFutureDone,
    EventsStreamRtSees(u32),
}

#[derive(Debug)]
pub struct ReadJobTrace {
    jobid: u64,
    ts0: Instant,
    events: Vec<(Instant, ReadEventKind)>,
}

impl ReadJobTrace {
    pub fn new() -> Self {
        static JOBID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self {
            jobid: JOBID.fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            ts0: Instant::now(),
            events: Vec::with_capacity(128),
        }
    }

    pub fn add_event_now(&mut self, kind: ReadEventKind) {
        self.events.push((Instant::now(), kind))
    }
}

impl fmt::Display for ReadJobTrace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "ReadJobTrace  jobid {jid}", jid = self.jobid)?;
        for (ts, kind) in &self.events {
            let dt = 1e3 * ts.saturating_duration_since(self.ts0).as_secs_f32();
            write!(fmt, "\njobid {jid:4}  {dt:7.2}  {kind:?}", jid = self.jobid)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct ReadNextValuesOpts {
    rt: RetentionTime,
    series: u64,
    ts_msp: TsMs,
    range: ScyllaSeriesRange,
    fwd: bool,
    readopts: EventReadOpts,
    scyqueue: ScyllaQueue,
}

impl ReadNextValuesOpts {
    pub(super) fn new(
        rt: RetentionTime,
        series: SeriesId,
        ts_msp: TsMs,
        range: ScyllaSeriesRange,
        fwd: bool,
        readopts: EventReadOpts,
        scyqueue: ScyllaQueue,
    ) -> Self {
        Self {
            rt,
            series: series.id(),
            ts_msp,
            range,
            fwd,
            readopts,
            scyqueue,
        }
    }
}

pub(super) struct ReadNextValuesParams {
    pub opts: ReadNextValuesOpts,
    pub jobtrace: ReadJobTrace,
}

pub(super) async fn read_next_values<ST>(params: ReadNextValuesParams) -> Result<(Box<dyn Events>, ReadJobTrace), Error>
where
    ST: ValTy,
{
    let opts = params.opts;
    let jobtrace = params.jobtrace;
    // TODO could take scyqeue out of opts struct.
    let scyqueue = opts.scyqueue.clone();
    let level = taskrun::query_log_level();
    let futgen = move |scy: Arc<Session>, stmts: Arc<StmtsEvents>, mut jobtrace: ReadJobTrace| {
        // TODO avoid this
        // opts.jobtrace = jobtrace;
        let fut = async move {
            // let jobtrace = &mut opts.jobtrace;
            let logspan = if level == Level::DEBUG {
                tracing::span!(Level::INFO, "log_span_debug")
            } else if level == Level::TRACE {
                tracing::span!(Level::INFO, "log_span_trace")
            } else {
                tracing::Span::none()
            };
            jobtrace.add_event_now(ReadEventKind::FutgenCallingReadNextValues);
            let fut = ST::read_next_values_trait(opts, jobtrace, scy, stmts).instrument(logspan);
            match fut.await.map_err(crate::worker::Error::from) {
                Ok((ret, mut jobtrace)) => {
                    jobtrace.add_event_now(ReadEventKind::ReadNextValuesFutureDone);
                    Ok((ret, jobtrace))
                }
                Err(e) => Err(e),
            }
        };
        Box::pin(fut)
            as Pin<Box<dyn Future<Output = Result<(Box<dyn Events>, ReadJobTrace), crate::worker::Error>> + Send>>
    };
    let (res, jobtrace) = scyqueue.read_next_values(futgen, jobtrace).await?;
    Ok((res, jobtrace))
}

async fn read_next_values_2<ST>(
    opts: ReadNextValuesOpts,
    mut jobtrace: ReadJobTrace,
    scy: Arc<Session>,
    stmts: Arc<StmtsEvents>,
) -> Result<(Box<dyn Events>, ReadJobTrace), Error>
where
    ST: ValTy,
{
    let use_method_2 = true;

    trace!("read_next_values_2  {:?}  st_name {}", opts, ST::st_name());
    let series = opts.series;
    let ts_msp = opts.ts_msp;
    let range = opts.range;
    let table_name = ST::table_name();
    let with_values = opts.readopts.with_values();
    if range.end() > TsNano::from_ns(i64::MAX as u64) {
        return Err(Error::RangeEndOverflow);
    }
    let ret = if opts.fwd {
        let ts_lsp_min = if range.beg() > ts_msp.ns() {
            range.beg().delta(ts_msp.ns())
        } else {
            DtNano::from_ns(0)
        };
        let ts_lsp_max = if range.end() > ts_msp.ns() {
            range.end().delta(ts_msp.ns())
        } else {
            DtNano::from_ns(0)
        };
        trace!(
            "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  {}",
            ts_msp.fmt(),
            ts_lsp_min,
            ts_lsp_max,
            table_name,
        );
        let qu = stmts
            .rt(&opts.rt)
            .lsp(!opts.fwd, with_values)
            .shape(ST::is_valueblob())
            .st(ST::st_name())?;
        let qu = {
            let mut qu = qu.clone();
            if qu.is_token_aware() == false {
                return Err(Error::NotTokenAware);
            }
            qu.set_page_size(10000);
            // qu.disable_paging();
            qu
        };
        let params = (
            series as i64,
            ts_msp.ms() as i64,
            ts_lsp_min.ns() as i64,
            ts_lsp_max.ns() as i64,
        );
        trace!("FWD event search  params {:?}", params);
        jobtrace.add_event_now(ReadEventKind::CallExecuteIter);
        let mut res = scy.execute_iter(qu.clone(), params).await?;
        if use_method_2 == false {
            let mut rows = Vec::new();
            while let Some(x) = res.next().await {
                rows.push(x?);
            }
            let mut last_before = None;
            let ret = <ST as ValTy>::convert_rows(rows, range, ts_msp, with_values, !opts.fwd, &mut last_before)?;
            ret
        } else {
            let mut ret = <ST as ValTy>::Container::empty();
            // TODO must branch already here depending on what input columns we expect
            if with_values {
                if <ST as ValTy>::is_valueblob() {
                    let mut it = res.into_typed::<(i64, Vec<u8>)>();
                    while let Some(x) = it.next().await {
                        let row = x?;
                        let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                        let value = <ST as ValTy>::from_valueblob(row.1);
                        ret.push(ts.ns(), 0, value);
                    }
                    ret
                } else {
                    let mut i = 0;
                    let mut it = res.into_typed::<(i64, ST::ScyTy)>();
                    while let Some(x) = it.next().await {
                        let row = x?;
                        let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                        let value = <ST as ValTy>::from_scyty(row.1);
                        ret.push(ts.ns(), 0, value);
                        i += 1;
                        if i % 2000 == 0 {
                            jobtrace.add_event_now(ReadEventKind::ScyllaReadRow(i));
                        }
                    }
                    {
                        jobtrace.add_event_now(ReadEventKind::ScyllaReadRowDone(i));
                    }
                    ret
                }
            } else {
                let mut it = res.into_typed::<(i64,)>();
                while let Some(x) = it.next().await {
                    let row = x?;
                    let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                    let value = <ST as ValTy>::default();
                    ret.push(ts.ns(), 0, value);
                }
                ret
            }
        }
    } else {
        let ts_lsp_max = if ts_msp.ns() < range.beg() {
            range.beg().delta(ts_msp.ns())
        } else {
            DtNano::from_ns(0)
        };
        trace!(
            "BCK  ts_msp {}  ts_lsp_max {}  {}",
            ts_msp.fmt(),
            ts_lsp_max,
            table_name,
        );
        let qu = stmts
            .rt(&opts.rt)
            .lsp(!opts.fwd, with_values)
            .shape(ST::is_valueblob())
            .st(ST::st_name())?;
        let params = (series as i64, ts_msp.ms() as i64, ts_lsp_max.ns() as i64);
        trace!("BCK event search  params {:?}", params);
        let mut res = scy.execute_iter(qu.clone(), params).await?;
        let mut rows = Vec::new();
        while let Some(x) = res.next().await {
            rows.push(x?);
        }
        let mut _last_before = None;
        let ret = ST::convert_rows(rows, range, ts_msp, with_values, !opts.fwd, &mut _last_before)?;
        if ret.len() > 1 {
            error!("multiple events in backwards search {}", ret.len());
        }
        ret
    };
    trace!("read  ts_msp {}  len {}", ts_msp.fmt(), ret.len());
    let ret = Box::new(ret);
    Ok((ret, jobtrace))
}

fn convert_rows_0<ST: ValTy>(
    rows: Vec<Row>,
    range: ScyllaSeriesRange,
    ts_msp: TsMs,
    with_values: bool,
    bck: bool,
    last_before: &mut Option<(TsNano, ST)>,
) -> Result<<ST as ValTy>::Container, Error> {
    let mut ret = <ST as ValTy>::Container::empty();
    for row in rows {
        let (ts, value) = if with_values {
            if ST::is_valueblob() {
                let row: (i64, Vec<u8>) = row.into_typed()?;
                // trace!("read a value blob len {}", row.1.len());
                let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                let value = ValTy::from_valueblob(row.1);
                (ts, value)
            } else {
                let row: (i64, ST::ScyTy) = row.into_typed()?;
                let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                let value = ValTy::from_scyty(row.1);
                (ts, value)
            }
        } else {
            let row: (i64,) = row.into_typed()?;
            let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
            let value = ValTy::default();
            (ts, value)
        };
        if bck {
            if ts >= range.beg() {
                // TODO count as logic error
                error!("ts >= range.beg");
            } else if ts < range.beg() {
                ret.push(ts.ns(), 0, value);
            } else {
                *last_before = Some((ts, value));
            }
        } else {
            if ts >= range.end() {
                // TODO count as logic error
                error!("ts >= range.end");
            } else if ts >= range.beg() {
                ret.push(ts.ns(), 0, value);
            } else {
                if last_before.is_none() {
                    warn!("encounter event before range in forward read {ts}");
                }
                *last_before = Some((ts, value));
            }
        }
    }
    Ok(ret)
}

fn convert_rows_enum(
    rows: Vec<Row>,
    range: ScyllaSeriesRange,
    ts_msp: TsMs,
    with_values: bool,
    bck: bool,
    last_before: &mut Option<(TsNano, EnumVariant)>,
) -> Result<<EnumVariant as ValTy>::Container, Error> {
    let mut ret = <EnumVariant as ValTy>::Container::empty();
    trace_fetch!("convert_rows_enum  {}", <EnumVariant as ValTy>::st_name());
    for row in rows {
        let (ts, value) = if with_values {
            if EnumVariant::is_valueblob() {
                return Err(Error::Logic);
            } else {
                let row: (i64, i16, String) = row.into_typed()?;
                let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                let val = row.1 as u16;
                let valstr = row.2;
                let value = EnumVariant::new(val, valstr);
                // trace_fetch!("read enum variant  {:?}  {:?}", value, value.name_string());
                (ts, value)
            }
        } else {
            let row: (i64,) = row.into_typed()?;
            let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
            let value = ValTy::default();
            (ts, value)
        };
        if bck {
            if ts >= range.beg() {
                // TODO count as logic error
                error!("ts >= range.beg");
            } else if ts < range.beg() {
                ret.push(ts.ns(), 0, value);
            } else {
                *last_before = Some((ts, value));
            }
        } else {
            if ts >= range.end() {
                // TODO count as logic error
                error!("ts >= range.end");
            } else if ts >= range.beg() {
                ret.push(ts.ns(), 0, value);
            } else {
                if last_before.is_none() {
                    warn!("encounter event before range in forward read {ts}");
                }
                *last_before = Some((ts, value));
            }
        }
    }
    trace_fetch!("convert_rows_enum  return {:?}", ret);
    Ok(ret)
}
