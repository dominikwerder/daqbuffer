use err::Error;
use futures_core::{Future, Stream};
use futures_util::FutureExt;
use items::scalarevents::ScalarEvents;
use items::waveevents::WaveEvents;
use items::{Framable, RangeCompletableItem, StreamItem};
use netpod::log::*;
use netpod::query::RawEventsQuery;
use netpod::{NanoRange, ScalarType, ScyllaConfig};
use scylla::frame::response::cql_to_rust::FromRowError as ScyFromRowError;
use scylla::transport::errors::{NewSessionError as ScyNewSessionError, QueryError as ScyQueryError};
use scylla::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T> ErrConv<T> for Result<T, ScyQueryError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, ScyNewSessionError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, ScyFromRowError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{e:?}"))),
        }
    }
}

macro_rules! impl_read_values_fut {
    ($fname:ident, $self:expr, $ts_msp:expr) => {{
        let fut = $fname($self.series, $ts_msp, $self.range.clone(), $self.scy.clone());
        let fut = fut.map(|x| {
            let x2 = match x {
                Ok(k) => {
                    //
                    Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
                }
                Err(e) => {
                    //
                    Err(e)
                }
            };
            //Box::new(Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))) as Box<dyn Framable + Send>});
            let ret = Box::new(x2) as Box<dyn Framable + 'static>;
            ret
        });
        let fut = Box::pin(fut) as Pin<Box<dyn Future<Output = Box<dyn Framable>> + Send>>;
        fut
    }};
}

struct ReadValues {
    series: i64,
    scalar_type: ScalarType,
    range: NanoRange,
    ts_msp: VecDeque<u64>,
    fut: Pin<Box<dyn Future<Output = Box<dyn Framable>> + Send>>,
    scy: Arc<ScySession>,
}

impl ReadValues {
    fn new(
        series: i64,
        scalar_type: ScalarType,
        range: NanoRange,
        ts_msp: VecDeque<u64>,
        scy: Arc<ScySession>,
    ) -> Self {
        Self {
            series,
            scalar_type,
            range,
            ts_msp,
            fut: Box::pin(futures_util::future::lazy(|_| panic!())),
            scy,
        }
    }

    fn next(&mut self) -> bool {
        if let Some(ts_msp) = self.ts_msp.pop_front() {
            self.fut = self.make_fut(ts_msp);
            true
        } else {
            false
        }
    }

    fn make_fut(&mut self, ts_msp: u64) -> Pin<Box<dyn Future<Output = Box<dyn Framable>> + Send>> {
        // TODO this also needs to differentiate on Shape.
        let fut = match &self.scalar_type {
            ScalarType::F32 => {
                impl_read_values_fut!(read_next_values_scalar_f32, self, ts_msp)
            }
            ScalarType::F64 => {
                impl_read_values_fut!(read_next_values_scalar_f64, self, ts_msp)
            }
            _ => err::todoval(),
        };
        fut
    }
}

enum FrState {
    New,
    FindSeries(Pin<Box<dyn Future<Output = Result<(i64, ScalarType), Error>> + Send>>),
    FindMsp(Pin<Box<dyn Future<Output = Result<Vec<u64>, Error>> + Send>>),
    ReadValues(ReadValues),
    Done,
}

pub struct ScyllaFramableStream {
    state: FrState,
    facility: String,
    channel_name: String,
    range: NanoRange,
    scalar_type: Option<ScalarType>,
    series: i64,
    scy: Arc<ScySession>,
}

impl ScyllaFramableStream {
    pub fn new(evq: &RawEventsQuery, scy: Arc<ScySession>) -> Self {
        Self {
            state: FrState::New,
            facility: evq.channel.backend.clone(),
            channel_name: evq.channel.name().into(),
            range: evq.range.clone(),
            scalar_type: None,
            series: 0,
            scy,
        }
    }
}

impl Stream for ScyllaFramableStream {
    type Item = Box<dyn Framable>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.state {
                FrState::New => {
                    let fut = find_series(self.facility.clone(), self.channel_name.clone(), self.scy.clone());
                    let fut = Box::pin(fut);
                    self.state = FrState::FindSeries(fut);
                    continue;
                }
                FrState::FindSeries(ref mut fut) => match fut.poll_unpin(cx) {
                    Ready(Ok((series, scalar_type))) => {
                        info!("ScyllaFramableStream  found series {}", series);
                        self.series = series;
                        self.scalar_type = Some(scalar_type);
                        let fut = find_ts_msp(series, self.range.clone(), self.scy.clone());
                        let fut = Box::pin(fut);
                        self.state = FrState::FindMsp(fut);
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.state = FrState::Done;
                        Ready(Some(Box::new(
                            Err(e) as Result<StreamItem<RangeCompletableItem<ScalarEvents<f32>>>, _>
                        )))
                    }
                    Pending => Pending,
                },
                FrState::FindMsp(ref mut fut) => match fut.poll_unpin(cx) {
                    Ready(Ok(ts_msp)) => {
                        info!("found ts_msp {ts_msp:?}");
                        // TODO get rid of into() for VecDeque
                        let mut st = ReadValues::new(
                            self.series,
                            self.scalar_type.as_ref().unwrap().clone(),
                            self.range.clone(),
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
                        Ready(Some(Box::new(
                            Err(e) as Result<StreamItem<RangeCompletableItem<ScalarEvents<f32>>>, _>
                        )))
                    }
                    Pending => Pending,
                },
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(item) => {
                        if st.next() {
                        } else {
                            info!("ReadValues exhausted");
                            self.state = FrState::Done;
                        }
                        Ready(Some(item))
                    }
                    Pending => Pending,
                },
                FrState::Done => Ready(None),
            };
        }
    }
}

async fn find_series(facility: String, channel_name: String, scy: Arc<ScySession>) -> Result<(i64, ScalarType), Error> {
    info!("find_series");
    let res = {
        let cql =
            "select series, scalar_type, shape_dims from series_by_channel where facility = ? and channel_name = ?";
        scy.query(cql, (&facility, &channel_name)).await.err_conv()?
    };
    let rows: Vec<_> = res.rows_typed_or_empty::<(i64, i32, Vec<i32>)>().collect();
    if rows.len() > 1 {
        error!("Multiple series found for channel, can not return data for ambiguous series");
        return Err(Error::with_public_msg_no_trace(
            "Multiple series found for channel, can not return data for ambiguous series",
        ));
    }
    if rows.len() < 1 {
        return Err(Error::with_public_msg_no_trace(
            "Multiple series found for channel, can not return data for ambiguous series",
        ));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::with_public_msg_no_trace(format!("can not find series for channel")))?
        .err_conv()?;
    info!("make_scylla_stream  row {row:?}");
    let series = row.0;
    let scalar_type = ScalarType::from_scylla_i32(row.1)?;
    info!("make_scylla_stream  series {series}");
    Ok((series, scalar_type))
}

async fn find_ts_msp(series: i64, range: NanoRange, scy: Arc<ScySession>) -> Result<Vec<u64>, Error> {
    let cql = "select ts_msp from ts_msp where series = ? and ts_msp >= ? and ts_msp < ?";
    let res = scy
        .query(cql, (series, range.beg as i64, range.end as i64))
        .await
        .err_conv()?;
    let mut ret = vec![];
    for row in res.rows_typed_or_empty::<(i64,)>() {
        let row = row.err_conv()?;
        ret.push(row.0 as u64);
    }
    info!("found in total {} rows", ret.len());
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
            info!("{}  series {}  ts_msp {}", stringify!($fname), series, ts_msp);
            // TODO add the constraint on range!
            warn!("remove the limit clause, add range check");
            // TODO use typed timestamp..
            let ts_lsp_max = if range.end < ts_msp {
                // We should not be here anyway.
                warn!("range.end < ts_msp");
                0
            } else {
                range.end - ts_msp
            };
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
            for row in res.rows_typed_or_empty::<(i64, i64, SCYTY)>() {
                let row = row.err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = row.2 as ST;
                ret.push(ts, pulse, value);
            }
            info!("found in total {} events  ts_msp {}", ret.tss.len(), ts_msp);
            Ok(ret)
        }
    };
}

macro_rules! read_next_1d_values {
    ($fname:ident, $st:ty, $scyty:ty, $table_name:expr) => {
        async fn $fname(
            series: i64,
            ts_msp: u64,
            range: NanoRange,
            scy: Arc<ScySession>,
        ) -> Result<WaveEvents<$st>, Error> {
            type ST = $st;
            type SCYTY = $scyty;
            info!("{}  series {}  ts_msp {}", stringify!($fname), series, ts_msp);
            // TODO add the constraint on range!
            warn!("remove the limit clause, add range check");
            // TODO use typed timestamp..
            let ts_lsp_max = if range.end < ts_msp {
                // We should not be here anyway.
                warn!("range.end < ts_msp");
                0
            } else {
                range.end - ts_msp
            };
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
            for row in res.rows_typed_or_empty::<(i64, i64, SCYTY)>() {
                let row = row.err_conv()?;
                let ts = ts_msp + row.0 as u64;
                let pulse = row.1 as u64;
                let value = row.2 as ST;
                ret.push(ts, pulse, value);
            }
            info!("found in total {} events  ts_msp {}", ret.tss.len(), ts_msp);
            Ok(ret)
        }
    };
}

read_next_scalar_values!(read_next_values_scalar_f32, f32, f32, "events_scalar_f32");
read_next_scalar_values!(read_next_values_scalar_f64, f64, f64, "events_scalar_f64");

read_next_1d_values!(read_next_values_1d_u16, u16, u16, "events_wave_u16");

pub async fn make_scylla_stream(
    evq: &RawEventsQuery,
    scyco: &ScyllaConfig,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    info!("make_scylla_stream open scylla connection");
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyco.hosts)
        .use_keyspace(&scyco.keyspace, true)
        .build()
        .await
        .err_conv()?;
    let scy = Arc::new(scy);
    let res = Box::pin(ScyllaFramableStream::new(evq, scy)) as _;
    Ok(res)
}

pub async fn make_scylla_stream_2(
    evq: &RawEventsQuery,
    scyco: &ScyllaConfig,
) -> Result<Pin<Box<dyn Stream<Item = Box<dyn Framable>> + Send>>, Error> {
    // Find the "series" id.
    info!("make_scylla_stream finding series id");
    let scy = scylla::SessionBuilder::new()
        .known_nodes(&scyco.hosts)
        .use_keyspace(&scyco.keyspace, true)
        .build()
        .await
        .err_conv()?;
    let res = {
        let cql =
            "select series, scalar_type, shape_dims from series_by_channel where facility = ? and channel_name = ?";
        scy.query(cql, (&evq.channel.backend, evq.channel.name()))
            .await
            .err_conv()?
    };
    let rows: Vec<_> = res.rows_typed_or_empty::<(i64, i32, Vec<i32>)>().collect();
    if rows.len() > 1 {
        error!("Multiple series found for channel, can not return data for ambiguous series");
        return Err(Error::with_public_msg_no_trace(
            "Multiple series found for channel, can not return data for ambiguous series",
        ));
    }
    if rows.len() < 1 {
        return Err(Error::with_public_msg_no_trace(
            "Multiple series found for channel, can not return data for ambiguous series",
        ));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::with_public_msg_no_trace(format!("can not find series for channel")))?
        .err_conv()?;
    info!("make_scylla_stream  row {row:?}");
    let series = row.0;
    info!("make_scylla_stream  series {series}");
    let _expand = evq.agg_kind.need_expand();
    let range = &evq.range;
    {
        let cql = "select ts_msp from ts_msp where series = ? and ts_msp >= ? and ts_msp < ?";
        let res = scy
            .query(cql, (series, range.beg as i64, range.end as i64))
            .await
            .err_conv()?;
        let mut rc = 0;
        for _row in res.rows_or_empty() {
            rc += 1;
        }
        info!("found in total {} rows", rc);
    }
    error!("TODO scylla fetch continue here");
    let res = Box::pin(futures_util::stream::empty());
    Ok(res)
}
