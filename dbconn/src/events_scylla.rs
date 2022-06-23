use crate::ErrConv;
use err::Error;
use futures_util::{Future, FutureExt, Stream};
use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use items::{EventsDyn,  RangeCompletableItem, Sitemty, StreamItem};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::{Database, NanoRange, ScalarType, ScyllaConfig, Shape};
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_postgres::Client as PgClient;

macro_rules! impl_read_values_fut {
    ($fname:ident, $self:expr, $ts_msp:expr) => {{
        let fut = $fname($self.series, $ts_msp, $self.range.clone(), $self.scy.clone());
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
    ts_msp: VecDeque<u64>,
    fut: Pin<Box<dyn Future<Output = Result<Box<dyn EventsDyn>, Error>> + Send>>,
    scy: Arc<ScySession>,
}

impl ReadValues {
    fn new(
        series: i64,
        scalar_type: ScalarType,
        shape: Shape,
        range: NanoRange,
        ts_msp: VecDeque<u64>,
        scy: Arc<ScySession>,
    ) -> Self {
        Self {
            series,
            scalar_type,
            shape,
            range,
            ts_msp,
            fut: Box::pin(futures_util::future::lazy(|_| panic!())),
            scy,
        }
    }

    fn next(&mut self) -> bool {
        if let Some(ts_msp) = self.ts_msp.pop_front() {
            self.fut = self.make_fut(ts_msp, self.ts_msp.len() > 1);
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
        // TODO this also needs to differentiate on Shape.
        let fut = match &self.shape {
            Shape::Scalar => match &self.scalar_type {
                ScalarType::I32 => {
                    impl_read_values_fut!(read_next_values_scalar_i32, self, ts_msp)
                }
                ScalarType::F32 => {
                    impl_read_values_fut!(read_next_values_scalar_f32, self, ts_msp)
                }
                ScalarType::F64 => {
                    impl_read_values_fut!(read_next_values_scalar_f64, self, ts_msp)
                }
                _ => err::todoval(),
            },
            Shape::Wave(_) => match &self.scalar_type {
                ScalarType::U16 => {
                    impl_read_values_fut!(read_next_values_array_u16, self, ts_msp)
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
    FindMsp(Pin<Box<dyn Future<Output = Result<Vec<u64>, Error>> + Send>>),
    ReadValues(ReadValues),
    Done,
}

pub struct EventsStreamScylla {
    state: FrState,
    #[allow(unused)]
    evq: RawEventsQuery,
    scalar_type: ScalarType,
    shape: Shape,
    series: u64,
    range: NanoRange,
    scy: Arc<ScySession>,
    do_test_stream_error: bool,
}

impl EventsStreamScylla {
    pub fn new(
        evq: &RawEventsQuery,
        scalar_type: ScalarType,
        shape: Shape,
        scy: Arc<ScySession>,
        do_test_stream_error: bool,
    ) -> Self {
        Self {
            state: FrState::New,
            series: evq.channel.series.unwrap(),
            evq: evq.clone(),
            scalar_type,
            shape,
            range: evq.range.clone(),
            scy,
            do_test_stream_error,
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
                    Ready(Ok(ts_msp)) => {
                        info!("found ts_msp {ts_msp:?}");
                        // TODO get rid of into() for VecDeque
                        let mut st = ReadValues::new(
                            self.series as i64,
                            self.scalar_type.clone(),
                            self.shape.clone(),
                            self.range.clone(),
                            // TODO get rid of the conversion:
                            ts_msp.into(),
                            self.scy.clone(),
                        );
                        if st.next() {
                            self.state = FrState::ReadValues(st);
                        } else {
                            self.state = FrState::Done;
                        }
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.state = FrState::Done;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(item)) => {
                        if st.next() {
                        } else {
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

async fn find_series(series: u64, pgclient: Arc<PgClient>) -> Result<(ScalarType, Shape), Error> {
    info!("find_series  series {}", series);
    let rows = {
        let q = "select facility, channel, scalar_type, shape_dims from series_by_channel where series = $1";
        pgclient.query(q, &[&(series as i64)]).await.err_conv()?
    };
    if rows.len() < 1 {
        return Err(Error::with_public_msg_no_trace(
            "Multiple series found for channel, can not return data for ambiguous series",
        ));
    }
    if rows.len() > 1 {
        error!("Multiple series found for channel, can not return data for ambiguous series");
        return Err(Error::with_public_msg_no_trace(
            "Multiple series found for channel, can not return data for ambiguous series",
        ));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::with_public_msg_no_trace(format!("can not find series for channel")))?;
    info!("row {row:?}");
    let _facility: String = row.get(0);
    let _channel: String = row.get(1);
    let a: i32 = row.get(2);
    let scalar_type = ScalarType::from_scylla_i32(a)?;
    let a: Vec<i32> = row.get(3);
    let shape = Shape::from_scylla_shape_dims(&a)?;
    info!("make_scylla_stream  series {series}  scalar_type {scalar_type:?}  shape {shape:?}");
    Ok((scalar_type, shape))
}

async fn find_ts_msp(series: i64, range: NanoRange, scy: Arc<ScySession>) -> Result<Vec<u64>, Error> {
    trace!("find_ts_msp  series {}  {:?}", series, range);
    // TODO use prepared statements
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp < ? order by ts_msp desc limit 1";
    let res = scy.query(cql, (series, range.beg as i64)).await.err_conv()?;
    let mut before = vec![];
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        before.push(row.0 as u64);
    }
    trace!("FOUND BEFORE THE REQUESTED TIME:  {}  {:?}", before.len(), before);
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp >= ? and ts_msp < ?";
    let res = scy
        .query(cql, (series, range.beg as i64, range.end as i64))
        .await
        .err_conv()?;
    let mut ret = vec![];
    for x in before {
        ret.push(x);
    }
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret.push(row.0 as u64);
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
            scy: Arc<ScySession>,
        ) -> Result<ScalarEvents<$st>, Error> {
            type ST = $st;
            type SCYTY = $scyty;
            trace!("{}  series {}  ts_msp {}", stringify!($fname), series, ts_msp);
            let _ts_lsp_max = if range.end <= ts_msp {
                // TODO we should not be here...
            } else {
            };
            if range.end > i64::MAX as u64 {
                return Err(Error::with_msg_no_trace(format!("range.end overflows i64")));
            }
            let ts_lsp_max = range.end;
            let cql = concat!(
                "select ts_lsp, pulse, value from ",
                $table_name,
                " where series = ? and ts_msp = ? and ts_lsp < ?"
            );
            let res = scy
                .query(cql, (series, ts_msp as i64, ts_lsp_max as i64))
                .await
                .err_conv()?;
            let mut ret = ScalarEvents::<ST>::empty();
            let mut discarded = 0;
            for row in res.rows_typed_or_empty::<(i64, i64, SCYTY)>() {
                let row = row.err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = row.2 as ST;
                if ts < range.beg || ts >= range.end {
                    discarded += 1;
                } else {
                    ret.push(ts, pulse, value);
                }
            }
            trace!(
                "found in total {} events  ts_msp {}  discarded {}",
                ret.tss.len(),
                ts_msp,
                discarded
            );
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
            scy: Arc<ScySession>,
        ) -> Result<WaveEvents<$st>, Error> {
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

read_next_scalar_values!(read_next_values_scalar_i32, i32, i32, "events_scalar_i32");
read_next_scalar_values!(read_next_values_scalar_f32, f32, f32, "events_scalar_f32");
read_next_scalar_values!(read_next_values_scalar_f64, f64, f64, "events_scalar_f64");

read_next_array_values!(read_next_values_array_u16, u16, i16, "events_wave_u16");

pub async fn make_scylla_stream(
    evq: &RawEventsQuery,
    scyco: &ScyllaConfig,
    dbconf: Database,
    do_test_stream_error: bool,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn EventsDyn>>> + Send>>, Error> {
    info!("make_scylla_stream open scylla connection");
    // TODO should RawEventsQuery already contain ScalarType and Shape?
    let (scalar_type, shape) = {
        let u = {
            let d = &dbconf;
            format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name)
        };
        let (pgclient, pgconn) = tokio_postgres::connect(&u, tokio_postgres::NoTls).await.err_conv()?;
        // TODO use common connection/pool:
        tokio::spawn(pgconn);
        let pgclient = Arc::new(pgclient);
        find_series(evq.channel.series.unwrap(), pgclient.clone()).await?
    };
    // TODO reuse existing connection:
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyco.hosts)
        .use_keyspace(&scyco.keyspace, true)
        .build()
        .await
        .err_conv()?;
    let scy = Arc::new(scy);
    let res = Box::pin(EventsStreamScylla::new(
        evq,
        scalar_type,
        shape,
        scy,
        do_test_stream_error,
    )) as _;
    Ok(res)
}
