use crate::errconv::ErrConv;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::scalar_ops::ScalarOps;
use items_0::Appendable;
use items_0::Empty;
use items_0::Events;
use items_0::WithLen;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use netpod::log::*;
use netpod::DtNano;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use scylla::frame::response::result::Row;
use scylla::prepared_statement::PreparedStatement;
use scylla::Session;
use scylla::Session as ScySession;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

#[derive(Debug)]
pub struct StmtsEventsRt {
    ts_msp_bck: PreparedStatement,
    ts_msp_fwd: PreparedStatement,
    read_value_queries: BTreeMap<String, PreparedStatement>,
}

impl StmtsEventsRt {
    pub(super) async fn new(rtpre: &str, scy: &Session) -> Result<Self, Error> {
        let cql = format!(
            "select ts_msp from {}{} where series = ? and ts_msp < ? order by ts_msp desc limit 2",
            rtpre, "ts_msp"
        );
        let ts_msp_bck = scy.prepare(cql).await.err_conv()?;
        let cql = format!(
            "select ts_msp from {}{} where series = ? and ts_msp >= ? and ts_msp < ?",
            rtpre, "ts_msp"
        );
        let ts_msp_fwd = scy.prepare(cql).await.err_conv()?;
        let mut read_value_queries = BTreeMap::new();
        for sct in [
            "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "bool", "string",
        ] {
            let combinations = [
                ("timestamps", "scalar", "ts_lsp, pulse"),
                ("timestamps", "array", "ts_lsp, pulse"),
                ("values", "scalar", "ts_lsp, pulse, value"),
                ("valueblobs", "array", "ts_lsp, pulse, valueblob"),
            ];
            for com in combinations {
                let query_name = format!("{}_{}_{}_fwd", com.1, sct, com.0);
                let cql = format!(
                    concat!(
                        "select {} from {}events_{}_{}",
                        " where series = ? and ts_msp = ? and ts_lsp >= ? and ts_lsp < ?"
                    ),
                    com.2, rtpre, com.1, sct,
                );
                let qu = scy.prepare(cql).await.err_conv()?;
                read_value_queries.insert(query_name, qu);

                let query_name = format!("{}_{}_{}_bck", com.1, sct, com.0);
                let cql = format!(
                    concat!(
                        "select {} from {}events_{}_{}",
                        " where series = ? and ts_msp = ? and ts_lsp < ? order by ts_lsp desc limit 1"
                    ),
                    com.2, rtpre, com.1, sct,
                );
                let qu = scy.prepare(cql).await.err_conv()?;
                read_value_queries.insert(query_name, qu);
            }
        }
        let ret = Self {
            ts_msp_bck,
            ts_msp_fwd,
            read_value_queries,
        };
        Ok(ret)
    }
}

pub(super) async fn find_ts_msp_worker(
    series: u64,
    range: ScyllaSeriesRange,
    stmts: &StmtsEventsRt,
    scy: &ScySession,
) -> Result<(VecDeque<TsMs>, VecDeque<TsMs>), Error> {
    trace!("find_ts_msp  series {:?}  {:?}", series, range);
    let mut ret1 = VecDeque::new();
    let mut ret2 = VecDeque::new();
    let params = (series as i64, range.beg().ms() as i64);
    trace!("find_ts_msp  query 1  params {:?}", params);
    let mut res = scy
        .execute_iter(stmts.ts_msp_bck.clone(), params)
        .await
        .err_conv()?
        .into_typed::<(i64,)>();
    while let Some(x) = res.next().await {
        let row = x.map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let ts = TsMs::from_ms_u64(row.0 as u64);
        trace!("query 1  ts_msp {}", ts);
        ret1.push_front(ts);
    }
    let params = (series as i64, range.beg().ms() as i64, 1 + range.end().ms() as i64);
    trace!("find_ts_msp  query 2  params {:?}", params);
    let mut res = scy
        .execute_iter(stmts.ts_msp_fwd.clone(), params)
        .await
        .err_conv()?
        .into_typed::<(i64,)>();
    while let Some(x) = res.next().await {
        let row = x.map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let ts = TsMs::from_ms_u64(row.0 as u64);
        trace!("query 2  ts_msp {}", ts);
        ret2.push_front(ts);
    }
    // let cql = "select ts_msp from st_ts_msp where series = ? and ts_msp >= ? limit 1";
    // let params = (series as i64, range.end().ms() as i64);
    // trace!("find_ts_msp  query 3  params {:?}", params);
    // let res = scy.query(cql, params).await.err_conv()?;
    // for row in res.rows_typed_or_empty::<(i64,)>() {
    //     let row = row.err_conv()?;
    //     let ts = TsMs::from_ms_u64(row.0 as u64);
    //     trace!("query 3  ts_msp {}", ts);
    //     ret2.push_back(ts);
    // }
    trace!("find_ts_msp  n1 {:?}  n2 {:?}", ret1.len(), ret2.len());
    Ok((ret1, ret2))
}

trait ValTy: Sized + 'static {
    type ScaTy: ScalarOps + std::default::Default;
    type ScyTy: scylla::cql_to_rust::FromCqlVal<scylla::frame::response::result::CqlValue>;
    type Container: Events + Appendable<Self>;
    fn from_scyty(inp: Self::ScyTy) -> Self;
    fn from_valueblob(inp: Vec<u8>) -> Self;
    fn table_name() -> &'static str;
    fn default() -> Self;
    fn is_valueblob() -> bool;
    fn st_name() -> &'static str;
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
                $table_name
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
                $table_name
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
        }
    };
}

impl ValTy for Vec<String> {
    type ScaTy = String;
    type ScyTy = Vec<String>;
    type Container = EventsDim1<String>;

    fn from_scyty(inp: Self::ScyTy) -> Self {
        inp
    }

    fn from_valueblob(inp: Vec<u8>) -> Self {
        todo!()
    }

    fn table_name() -> &'static str {
        "st_events_array_enum"
    }

    fn default() -> Self {
        Vec::new()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "enum"
    }
}

impl_scaty_scalar!(u8, i8, "u8", "st_events_scalar_u8");
impl_scaty_scalar!(u16, i16, "u16", "st_events_scalar_u16");
impl_scaty_scalar!(u32, i32, "u32", "st_events_scalar_u32");
impl_scaty_scalar!(u64, i64, "u64", "st_events_scalar_u64");
impl_scaty_scalar!(i8, i8, "i8", "st_events_scalar_i8");
impl_scaty_scalar!(i16, i16, "i16", "st_events_scalar_i16");
impl_scaty_scalar!(i32, i32, "i32", "st_events_scalar_i32");
impl_scaty_scalar!(i64, i64, "i64", "st_events_scalar_i64");
impl_scaty_scalar!(f32, f32, "f32", "st_events_scalar_f32");
impl_scaty_scalar!(f64, f64, "f64", "st_events_scalar_f64");
impl_scaty_scalar!(bool, bool, "bool", "st_events_scalar_bool");
impl_scaty_scalar!(String, String, "string", "st_events_scalar_string");

impl_scaty_array!(Vec<u8>, u8, Vec<i8>, "u8", "st_events_array_u8");
impl_scaty_array!(Vec<u16>, u16, Vec<i16>, "u16", "st_events_array_u16");
impl_scaty_array!(Vec<u32>, u32, Vec<i32>, "u32", "st_events_array_u32");
impl_scaty_array!(Vec<u64>, u64, Vec<i64>, "u64", "st_events_array_u64");
impl_scaty_array!(Vec<i8>, i8, Vec<i8>, "i8", "st_events_array_i8");
impl_scaty_array!(Vec<i16>, i16, Vec<i16>, "i16", "st_events_array_i16");
impl_scaty_array!(Vec<i32>, i32, Vec<i32>, "i32", "st_events_array_i32");
impl_scaty_array!(Vec<i64>, i64, Vec<i64>, "i64", "st_events_array_i64");
impl_scaty_array!(Vec<f32>, f32, Vec<f32>, "f32", "st_events_array_f32");
impl_scaty_array!(Vec<f64>, f64, Vec<f64>, "f64", "st_events_array_f64");
impl_scaty_array!(Vec<bool>, bool, Vec<bool>, "bool", "st_events_array_bool");

struct ReadNextValuesOpts {
    series: u64,
    ts_msp: TsMs,
    range: ScyllaSeriesRange,
    fwd: bool,
    with_values: bool,
    scyqueue: ScyllaQueue,
}

async fn read_next_values<ST>(opts: ReadNextValuesOpts) -> Result<Box<dyn Events>, Error>
where
    ST: ValTy,
{
    // TODO could take scyqeue out of opts struct.
    let scyqueue = opts.scyqueue.clone();
    let futgen = Box::new(|scy: Arc<ScySession>, stmts: Arc<StmtsEventsRt>| {
        let fut = read_next_values_worker::<ST>(opts, scy, stmts);
        Box::pin(fut) as Pin<Box<dyn Future<Output = Result<Box<dyn Events>, err::Error>> + Send>>
    });
    let res = scyqueue.read_next_values(futgen).await?;
    Ok(res)
}

async fn read_next_values_worker<ST>(
    opts: ReadNextValuesOpts,
    scy: Arc<ScySession>,
    stmts: Arc<StmtsEventsRt>,
) -> Result<Box<dyn Events>, Error>
where
    ST: ValTy,
{
    trace!("read_next_values_worker  {}  {}", opts.series, opts.ts_msp);
    let series = opts.series;
    let ts_msp = opts.ts_msp;
    let range = opts.range;
    let table_name = ST::table_name();
    if range.end() > TsNano::from_ns(i64::MAX as u64) {
        return Err(Error::with_msg_no_trace(format!("range.end overflows i64")));
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
            ts_msp,
            ts_lsp_min,
            ts_lsp_max,
            table_name,
        );
        let dir = "fwd";
        let qu_name = if opts.with_values {
            if ST::is_valueblob() {
                format!("array_{}_valueblobs_{}", ST::st_name(), dir)
            } else {
                format!("scalar_{}_values_{}", ST::st_name(), dir)
            }
        } else {
            if ST::is_valueblob() {
                format!("array_{}_timestamps_{}", ST::st_name(), dir)
            } else {
                format!("scalar_{}_timestamps_{}", ST::st_name(), dir)
            }
        };
        let qu = stmts.read_value_queries.get(&qu_name).ok_or_else(|| {
            let e = Error::with_msg_no_trace(format!("can not find query name {}", qu_name));
            error!("{e}");
            e
        })?;
        let params = (
            series as i64,
            ts_msp.ms() as i64,
            ts_lsp_min.ns() as i64,
            ts_lsp_max.ns() as i64,
        );
        trace!("FWD event search  params {:?}", params);
        let mut res = scy.execute_iter(qu.clone(), params).await.err_conv()?;
        let mut rows = Vec::new();
        while let Some(x) = res.next().await {
            rows.push(x.err_conv()?);
        }
        let mut last_before = None;
        let ret = convert_rows::<ST>(rows, range, ts_msp, opts.with_values, !opts.fwd, &mut last_before)?;
        ret
    } else {
        let ts_lsp_max = if ts_msp.ns() < range.beg() {
            range.beg().delta(ts_msp.ns())
        } else {
            DtNano::from_ns(0)
        };
        trace!("BCK  ts_msp {}  ts_lsp_max {}  {}", ts_msp, ts_lsp_max, table_name,);
        let dir = "bck";
        let qu_name = if opts.with_values {
            if ST::is_valueblob() {
                format!("array_{}_valueblobs_{}", ST::st_name(), dir)
            } else {
                format!("scalar_{}_values_{}", ST::st_name(), dir)
            }
        } else {
            if ST::is_valueblob() {
                format!("array_{}_timestamps_{}", ST::st_name(), dir)
            } else {
                format!("scalar_{}_timestamps_{}", ST::st_name(), dir)
            }
        };
        let qu = stmts.read_value_queries.get(&qu_name).ok_or_else(|| {
            let e = Error::with_msg_no_trace(format!("can not find query name {}", qu_name));
            error!("{e}");
            e
        })?;
        let params = (series as i64, ts_msp.ms() as i64, ts_lsp_max.ns() as i64);
        trace!("BCK event search  params {:?}", params);
        let mut res = scy.execute_iter(qu.clone(), params).await.err_conv()?;
        let mut rows = Vec::new();
        while let Some(x) = res.next().await {
            rows.push(x.err_conv()?);
        }
        let mut _last_before = None;
        let ret = convert_rows::<ST>(rows, range, ts_msp, opts.with_values, !opts.fwd, &mut _last_before)?;
        if ret.len() > 1 {
            error!("multiple events in backwards search {}", ret.len());
        }
        ret
    };
    trace!("read  ts_msp {:?}  len {}", ts_msp, ret.len());
    let ret = Box::new(ret);
    Ok(ret)
}

fn convert_rows<ST: ValTy>(
    rows: Vec<Row>,
    range: ScyllaSeriesRange,
    ts_msp: TsMs,
    with_values: bool,
    bck: bool,
    last_before: &mut Option<(TsNano, u64, ST)>,
) -> Result<<ST as ValTy>::Container, Error> {
    let mut ret = <ST as ValTy>::Container::empty();
    for row in rows {
        let (ts, pulse, value) = if with_values {
            if ST::is_valueblob() {
                let row: (i64, i64, Vec<u8>) = row.into_typed().err_conv()?;
                trace!("read a value blob len {}", row.2.len());
                let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                let pulse = row.1 as u64;
                let value = ValTy::from_valueblob(row.2);
                (ts, pulse, value)
            } else {
                let row: (i64, i64, ST::ScyTy) = row.into_typed().err_conv()?;
                let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                let pulse = row.1 as u64;
                let value = ValTy::from_scyty(row.2);
                (ts, pulse, value)
            }
        } else {
            let row: (i64, i64) = row.into_typed().err_conv()?;
            let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
            let pulse = row.1 as u64;
            let value = ValTy::default();
            (ts, pulse, value)
        };
        if bck {
            if ts >= range.beg() {
                // TODO count as logic error
                error!("ts >= range.beg");
            } else if ts < range.beg() {
                ret.push(ts.ns(), pulse, value);
            } else {
                *last_before = Some((ts, pulse, value));
            }
        } else {
            if ts >= range.end() {
                // TODO count as logic error
                error!("ts >= range.end");
            } else if ts >= range.beg() {
                ret.push(ts.ns(), pulse, value);
            } else {
                if last_before.is_none() {
                    warn!("encounter event before range in forward read {ts}");
                }
                *last_before = Some((ts, pulse, value));
            }
        }
    }
    Ok(ret)
}

struct ReadValues {
    series: u64,
    scalar_type: ScalarType,
    shape: Shape,
    range: ScyllaSeriesRange,
    ts_msps: VecDeque<TsMs>,
    fwd: bool,
    with_values: bool,
    fut: Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>>,
    fut_done: bool,
    scyqueue: ScyllaQueue,
}

impl ReadValues {
    fn new(
        series: u64,
        scalar_type: ScalarType,
        shape: Shape,
        range: ScyllaSeriesRange,
        ts_msps: VecDeque<TsMs>,
        fwd: bool,
        with_values: bool,
        scyqueue: ScyllaQueue,
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
            scyqueue,
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

    fn make_fut(&mut self, ts_msp: TsMs) -> Pin<Box<dyn Future<Output = Result<Box<dyn Events>, Error>> + Send>> {
        let opts = ReadNextValuesOpts {
            series: self.series.clone(),
            ts_msp,
            range: self.range.clone(),
            fwd: self.fwd,
            with_values: self.with_values,
            scyqueue: self.scyqueue.clone(),
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
                    ScalarType::Enum => read_next_values::<String>(opts).await,
                    ScalarType::ChannelStatus => {
                        warn!("read scalar channel status not yet supported");
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
                    ScalarType::STRING => {
                        warn!("read array string not yet supported");
                        err::todoval()
                    }
                    ScalarType::Enum => read_next_values::<Vec<String>>(opts).await,
                    ScalarType::ChannelStatus => {
                        warn!("read array channel status not yet supported");
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
    FindMsp(Pin<Box<dyn Future<Output = Result<(VecDeque<TsMs>, VecDeque<TsMs>), crate::worker::Error>> + Send>>),
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
    ts_msp_bck: VecDeque<TsMs>,
    ts_msp_fwd: VecDeque<TsMs>,
    scyqueue: ScyllaQueue,
    do_test_stream_error: bool,
    found_one_after: bool,
    with_values: bool,
    outqueue: VecDeque<Box<dyn Events>>,
    ts_seen_max: u64,
}

impl EventsStreamScylla {
    pub fn new(
        series: u64,
        range: ScyllaSeriesRange,
        do_one_before_range: bool,
        scalar_type: ScalarType,
        shape: Shape,
        with_values: bool,
        scyqueue: ScyllaQueue,
        do_test_stream_error: bool,
    ) -> Self {
        debug!("EventsStreamScylla::new");
        Self {
            state: FrState::New,
            series,
            scalar_type,
            shape,
            range,
            do_one_before_range,
            ts_msp_bck: VecDeque::new(),
            ts_msp_fwd: VecDeque::new(),
            scyqueue,
            do_test_stream_error,
            found_one_after: false,
            with_values,
            outqueue: VecDeque::new(),
            ts_seen_max: 0,
        }
    }

    fn ts_msps_found(&mut self, msps1: VecDeque<TsMs>, msps2: VecDeque<TsMs>) {
        trace!("ts_msps_found  msps1 {msps1:?}  msps2 {msps2:?}");
        self.ts_msp_bck = msps1;
        self.ts_msp_fwd = msps2;
        for x in self.ts_msp_bck.iter().rev() {
            let x = x.clone();
            if x.ns() >= self.range.end() {
                info!("FOUND one-after because of MSP");
                self.found_one_after = true;
            }
            self.ts_msp_fwd.push_front(x);
        }
        trace!("ts_msp_bck {:?}", self.ts_msp_bck);
        trace!("ts_msp_fwd {:?}", self.ts_msp_fwd);
        if let Some(msp) = self.ts_msp_bck.pop_back() {
            trace!("start ReadBack1  msp {}", msp);
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                [msp].into(),
                false,
                self.with_values,
                self.scyqueue.clone(),
            );
            self.state = FrState::ReadBack1(st);
        } else if self.ts_msp_fwd.len() > 0 {
            trace!("begin immediately with forward read");
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                true,
                self.with_values,
                self.scyqueue.clone(),
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
                trace!("start forward read after back1");
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                    true,
                    self.with_values,
                    self.scyqueue.clone(),
                );
                self.state = FrState::ReadValues(st);
            } else {
                self.state = FrState::DataDone;
            }
        } else {
            if let Some(msp) = self.ts_msp_bck.pop_back() {
                trace!("start ReadBack2  msp {}", msp);
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    [msp].into(),
                    false,
                    self.with_values,
                    self.scyqueue.clone(),
                );
                self.state = FrState::ReadBack2(st);
            } else if self.ts_msp_fwd.len() > 0 {
                trace!("no 2nd back MSP, go for forward read");
                let st = ReadValues::new(
                    self.series,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                    true,
                    self.with_values,
                    self.scyqueue.clone(),
                );
                self.state = FrState::ReadValues(st);
            } else {
                trace!("no 2nd back msp, but also nothing to go forward");
                self.state = FrState::DataDone;
            }
        }
    }

    fn back_2_done(&mut self, item: Box<dyn Events>) {
        trace!("back_2_done  item len {}", item.len());
        if item.len() > 0 {
            self.outqueue.push_back(item);
        }
        if self.ts_msp_fwd.len() > 0 {
            trace!("start forward read after back2");
            let st = ReadValues::new(
                self.series,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                mem::replace(&mut self.ts_msp_fwd, VecDeque::new()),
                true,
                self.with_values,
                self.scyqueue.clone(),
            );
            self.state = FrState::ReadValues(st);
        } else {
            trace!("nothing to forward read after back 2");
            self.state = FrState::DataDone;
        }
    }
}

async fn find_ts_msp_via_queue(
    series: u64,
    range: ScyllaSeriesRange,
    scyqueue: ScyllaQueue,
) -> Result<(VecDeque<TsMs>, VecDeque<TsMs>), crate::worker::Error> {
    scyqueue.find_ts_msp(series, range).await
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
                if let Some(item_min) = item.ts_min() {
                    if item_min < self.ts_seen_max {
                        debug!("ordering error A  {}  {}", item_min, self.ts_seen_max);
                    }
                }
                if let Some(item_max) = item.ts_max() {
                    if item_max < self.ts_seen_max {
                        debug!("ordering error B  {}  {}", item_max, self.ts_seen_max);
                    } else {
                        self.ts_seen_max = item_max;
                    }
                }
                debug!("deliver item  {}", item.output_info());
                break Ready(Some(Ok(ChannelEvents::Events(item))));
            }
            break match self.state {
                FrState::New => {
                    let series = self.series.clone();
                    let range = self.range.clone();
                    let fut = find_ts_msp_via_queue(series, range, self.scyqueue.clone());
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
                        error!("EventsStreamScylla  FindMsp  {e}");
                        self.state = FrState::DataDone;
                        Ready(Some(Err(e.into())))
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
                        error!("EventsStreamScylla  ReadBack1  {e}");
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
                        error!("EventsStreamScylla  ReadBack2  {e}");
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
                        error!("EventsStreamScylla  ReadValues  {e}");
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
