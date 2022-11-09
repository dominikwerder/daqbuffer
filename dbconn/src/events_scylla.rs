use crate::ErrConv;
use err::Error;
use futures_util::{Future, FutureExt, Stream, StreamExt};
use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use items::{EventsDyn, RangeCompletableItem, Sitemty, StreamItem};
use netpod::log::*;
use netpod::query::{ChannelStateEventsQuery, RawEventsQuery};
use netpod::timeunits::DAY;
use netpod::{Database, NanoRange, ScalarType, ScyllaConfig, Shape};
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

macro_rules! read_values {
    ($fname:ident, $self:expr, $ts_msp:expr) => {{
        let fut = $fname($self.series, $ts_msp, $self.range.clone(), $self.fwd, $self.scy.clone());
        let fut = fut.map(|x| {
            match x {
                Ok(k) => {
                    // TODO why static needed?
                    let b = Box::new(k) as Box<dyn EventsDyn + 'static>;
                    Ok(b)
                }
                Err(e) => Err(e),
            }
        });
        let fut = Box::pin(fut) as Pin<Box<dyn Future<Output = Result<Box<dyn EventsDyn>, Error>> + Send>>;
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
    fut: Pin<Box<dyn Future<Output = Result<Box<dyn EventsDyn>, Error>> + Send>>,
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
            scy,
        };
        ret.next();
        ret
    }

    fn next(&mut self) -> bool {
        if let Some(ts_msp) = self.ts_msps.pop_front() {
            self.fut = self.make_fut(ts_msp, self.ts_msps.len() > 1);
            true
        } else {
            false
        }
    }

    fn make_fut(
        &mut self,
        ts_msp: u64,
        _has_more_msp: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn EventsDyn>, Error>> + Send>> {
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
                _ => err::todoval(),
            },
            Shape::Wave(_) => match &self.scalar_type {
                ScalarType::U16 => {
                    read_values!(read_next_values_array_u16, self, ts_msp)
                }
                _ => err::todoval(),
            },
            _ => err::todoval(),
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
    #[allow(unused)]
    evq: RawEventsQuery,
    scalar_type: ScalarType,
    shape: Shape,
    range: NanoRange,
    ts_msps: VecDeque<u64>,
    scy: Arc<ScySession>,
    do_test_stream_error: bool,
}

impl EventsStreamScylla {
    pub fn new(
        series: u64,
        evq: &RawEventsQuery,
        scalar_type: ScalarType,
        shape: Shape,
        scy: Arc<ScySession>,
        do_test_stream_error: bool,
    ) -> Self {
        Self {
            state: FrState::New,
            series,
            evq: evq.clone(),
            scalar_type,
            shape,
            range: evq.range.clone(),
            ts_msps: VecDeque::new(),
            scy,
            do_test_stream_error,
        }
    }

    fn ts_msps_found(&mut self, ts_msps: VecDeque<u64>) {
        info!("found  ts_msps {ts_msps:?}");
        self.ts_msps = ts_msps;
        // Find the largest MSP which can potentially contain some event before the range.
        let befores: Vec<_> = self
            .ts_msps
            .iter()
            .map(|x| *x)
            .filter(|x| *x < self.range.beg)
            .collect();
        if befores.len() >= 1 {
            let st = ReadValues::new(
                self.series as i64,
                self.scalar_type.clone(),
                self.shape.clone(),
                self.range.clone(),
                [befores[befores.len() - 1]].into(),
                false,
                self.scy.clone(),
            );
            self.state = FrState::ReadBack1(st);
        } else if self.ts_msps.len() >= 1 {
            let st = ReadValues::new(
                self.series as i64,
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

    fn back_1_done(&mut self, item: Box<dyn EventsDyn>) -> Option<Box<dyn EventsDyn>> {
        info!("back_1_done  len {}", item.len());
        if item.len() == 0 {
            // Find the 2nd largest MSP which can potentially contain some event before the range.
            let befores: Vec<_> = self
                .ts_msps
                .iter()
                .map(|x| *x)
                .filter(|x| *x < self.range.beg)
                .collect();
            if befores.len() >= 2 {
                let st = ReadValues::new(
                    self.series as i64,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    [befores[befores.len() - 2]].into(),
                    false,
                    self.scy.clone(),
                );
                self.state = FrState::ReadBack2(st);
                None
            } else if self.ts_msps.len() >= 1 {
                let st = ReadValues::new(
                    self.series as i64,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    self.ts_msps.clone(),
                    true,
                    self.scy.clone(),
                );
                self.state = FrState::ReadValues(st);
                None
            } else {
                self.state = FrState::Done;
                None
            }
        } else {
            if self.ts_msps.len() > 0 {
                let st = ReadValues::new(
                    self.series as i64,
                    self.scalar_type.clone(),
                    self.shape.clone(),
                    self.range.clone(),
                    self.ts_msps.clone(),
                    true,
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

    fn back_2_done(&mut self, item: Box<dyn EventsDyn>) -> Option<Box<dyn EventsDyn>> {
        info!("back_2_done  len {}", item.len());
        if self.ts_msps.len() >= 1 {
            let st = ReadValues::new(
                self.series as i64,
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
        if item.len() > 0 {
            Some(item)
        } else {
            None
        }
    }
}

impl Stream for EventsStreamScylla {
    type Item = Sitemty<Box<dyn EventsDyn>>;

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
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
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
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
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
                        info!("read values");
                        item.verify();
                        item.output_info();
                        if !st.next() {
                            info!("ReadValues exhausted");
                            self.state = FrState::Done;
                        }
                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
                    }
                    Ready(Err(e)) => Ready(Some(Err(e))),
                    Pending => Pending,
                },
                FrState::Done => Ready(None),
            };
        }
    }
}

async fn find_ts_msp(_series: i64, _range: NanoRange, _scy: Arc<ScySession>) -> Result<VecDeque<u64>, Error> {
    // TODO remove
    panic!()
}

macro_rules! read_next_scalar_values {
    ($fname:ident, $st:ty, $scyty:ty, $table_name:expr) => {
        async fn $fname(
            series: i64,
            ts_msp: u64,
            range: NanoRange,
            fwd: bool,
            scy: Arc<ScySession>,
        ) -> Result<ScalarEvents<$st>, Error> {
            type ST = $st;
            type SCYTY = $scyty;
            if ts_msp >= range.end {
                warn!("given ts_msp {}  >=  range.end {}", ts_msp, range.end);
            }
            if range.end > i64::MAX as u64 {
                return Err(Error::with_msg_no_trace(format!("range.end overflows i64")));
            }
            let res = if fwd {
                let ts_lsp_min = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
                let ts_lsp_max = if ts_msp < range.end { range.end - ts_msp } else { 0 };
                trace!(
                    "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  {}",
                    ts_msp,
                    ts_lsp_min,
                    ts_lsp_max,
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
                    "BCK  ts_msp {}  ts_lsp_max {}  range beg {}  end {}  {}",
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
            let mut ret = ScalarEvents::<ST>::empty();
            for row in res.rows_typed_or_empty::<(i64, i64, SCYTY)>() {
                let row = row.err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = row.2 as ST;
                ret.push(ts, pulse, value);
            }
            trace!("found in total {} events  ts_msp {}", ret.tss.len(), ts_msp);
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
            scy: Arc<ScySession>,
        ) -> Result<WaveEvents<$st>, Error> {
            if true {
                return Err(Error::with_msg_no_trace("redo based on scalar case"));
            }
            type ST = $st;
            type SCYTY = $scyty;
            info!("{}  series {}  ts_msp {}", stringify!($fname), series, ts_msp);
            let cql = concat!(
                "select ts_lsp, pulse, value from ",
                $table_name,
                " where series = ? and ts_msp = ?"
            );
            let res = scy.query(cql, (series, ts_msp as i64)).await.err_conv()?;
            let mut ret = WaveEvents::<ST>::empty();
            for row in res.rows_typed_or_empty::<(i64, i64, Vec<SCYTY>)>() {
                let row = row.err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = row.2.into_iter().map(|x| x as ST).collect();
                ret.push(ts, pulse, value);
            }
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

// TODO remove
#[allow(unused)]
async fn make_scylla_stream(
    _evq: &RawEventsQuery,
    _scyco: &ScyllaConfig,
    _dbconf: Database,
    _do_test_stream_error: bool,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn EventsDyn>>> + Send>>, Error> {
    error!("forward call to crate scyllaconn");
    err::todoval()
}

pub async fn channel_state_events(
    evq: &ChannelStateEventsQuery,
    scyco: &ScyllaConfig,
) -> Result<Vec<(u64, u32)>, Error> {
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyco.hosts)
        .use_keyspace(&scyco.keyspace, true)
        .build()
        .await
        .err_conv()?;
    let scy = Arc::new(scy);
    let mut ret = Vec::new();
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
                ret.push((ts, kind));
            }
        }
        ts_msp += div;
        if ts_msp >= evq.range().end {
            break;
        }
    }
    Ok(ret)
}
