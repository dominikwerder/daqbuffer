use crate::ErrConv;
use chrono::DateTime;
use chrono::Utc;
use err::Error;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::ChConf;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use std::time::Duration;

/// It is an unsolved question as to how we want to uniquely address channels.
/// Currently, the usual (backend, channelname) works in 99% of the cases, but the edge-cases
/// are not solved. At the same time, it is desirable to avoid to complicate things for users.
/// Current state:
/// If the series id is given, we take that.
/// Otherwise we try to uniquely identify the series id from the given information.
/// In the future, we can even try to involve time range information for that, but backends like
/// old archivers and sf databuffer do not support such lookup.
pub async fn chconf_best_matching_for_name_and_range(
    backend: &str,
    name: &str,
    range: NanoRange,
    ncc: &NodeConfigCached,
) -> Result<ChConf, Error> {
    debug!("chconf_best_matching_for_name_and_range  {backend}  {name}  {range:?}");
    if ncc.node_config.cluster.scylla.is_none() {
        let e = Error::with_msg_no_trace(format!(
            "chconf_best_matching_for_name_and_range  but not a scylla backend"
        ));
        error!("{e}");
        return Err(e);
    };
    if backend != ncc.node_config.cluster.backend {
        warn!(
            "mismatched backend  {}  vs  {}",
            backend, ncc.node_config.cluster.backend
        );
    }
    let dbconf = &ncc.node_config.cluster.database;
    let pgclient = crate::create_connection(dbconf).await?;
    let sql = concat!(
        "select unnest(tscs) as tsc, series, scalar_type, shape_dims",
        " from series_by_channel",
        " where kind = 2 and facility = $1 and channel = $2",
        " order by tsc",
    );
    let res = pgclient.query(sql, &[&backend, &name]).await.err_conv()?;
    if res.len() == 0 {
        let e = Error::with_public_msg_no_trace(format!("can not find channel information for {name}"));
        warn!("{e}");
        Err(e)
    } else if res.len() > 1 {
        let mut rows = Vec::new();
        for r in res {
            let tsc: DateTime<Utc> = r.get(0);
            let series: i64 = r.get(1);
            let scalar_type: i32 = r.get(2);
            let shape_dims: Vec<i32> = r.get(3);
            let series = series as u64;
            let _scalar_type = ScalarType::from_scylla_i32(scalar_type)?;
            let _shape = Shape::from_scylla_shape_dims(&shape_dims)?;
            let tsms = tsc.signed_duration_since(DateTime::UNIX_EPOCH).num_milliseconds() as u64;
            let ts = TsMs(tsms);
            rows.push((ts, series));
        }
        let tsmss: Vec<_> = rows.iter().map(|x| x.0.clone()).collect();
        let range = (TsMs(range.beg / 1000), TsMs(range.end / 1000));
        let res = decide_best_matching_index(range, &tsmss)?;
        let ch_conf = chconf_for_series(backend, rows[res].1, ncc).await?;
        Ok(ch_conf)
    } else {
        let row = res.first().unwrap();
        let name: String = row.get(0);
        let series = row.get::<_, i64>(1) as u64;
        let scalar_type = ScalarType::from_dtype_index(row.get::<_, i32>(2) as u8)?;
        // TODO can I get a slice from psql driver?
        let shape = Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(3))?;
        let ret = ChConf::new(backend, series, scalar_type, shape, name);
        Ok(ret)
    }
}

fn decide_best_matching_index(range: (TsMs, TsMs), rows: &[TsMs]) -> Result<usize, Error> {
    if rows.len() < 1 {
        let e = Error::with_msg_no_trace("decide_best_matching_index  no rows");
        warn!("{e}");
        Err(e)
    } else {
        let rows: Vec<_> = rows
            .iter()
            .map(Clone::clone)
            .zip(rows[1..].iter().map(Clone::clone).chain([TsMs(u64::MAX)]))
            .map(|x| (x.0, x.1))
            .collect();
        let rows: Vec<_> = rows
            .into_iter()
            .map(|x| {
                let dur = if x.1 <= range.0 {
                    Duration::from_millis(0)
                } else if x.0 >= range.1 {
                    Duration::from_millis(0)
                } else if x.0 <= range.0 {
                    if x.1 >= range.1 {
                        Duration::from_millis((range.1.clone() - range.0.clone()).to_u64())
                    } else {
                        Duration::from_millis((x.1.clone() - range.0.clone()).to_u64())
                    }
                } else {
                    if x.1 >= range.1 {
                        Duration::from_millis((range.1.clone() - x.0.clone()).to_u64())
                    } else {
                        Duration::from_millis((x.1.clone() - x.0.clone()).to_u64())
                    }
                };
                dur
            })
            .collect();
        let mut max = Duration::ZERO;
        // By default, return the last
        let mut found = rows.len() - 1;
        for (i, r) in rows.into_iter().enumerate() {
            if r >= max {
                max = r;
                found = i;
            }
        }
        Ok(found)
    }
}

#[test]
fn test_decide_best_matching_index_before_00() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 0);
}

#[test]
fn test_decide_best_matching_index_before_01() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 1);
}

#[test]
fn test_decide_best_matching_index_before_02() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30), TsMs(70)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 1);
}

#[test]
fn test_decide_best_matching_index_overlap_00() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30), TsMs(42), TsMs(65)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 2);
}

#[test]
fn test_decide_best_matching_index_overlap_01() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30), TsMs(52), TsMs(58), TsMs(60)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 1);
}

#[test]
fn test_decide_best_matching_index_after_00() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(60)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 0);
}

#[test]
fn test_decide_best_matching_index_after_01() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(70)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 0);
}

pub async fn chconf_for_series(backend: &str, series: u64, ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    let dbconf = &ncc.node_config.cluster.database;
    let pgclient = crate::create_connection(dbconf).await?;
    let res = pgclient
        .query(
            "select channel, scalar_type, shape_dims from series_by_channel where facility = $1 and series = $2",
            &[&backend, &(series as i64)],
        )
        .await
        .err_conv()?;
    if res.len() < 1 {
        let e = Error::with_public_msg_no_trace(format!(
            "can not find channel information  backend {backend}  series {series}"
        ));
        warn!("{e}");
        Err(e)
    } else {
        let row = res.first().unwrap();
        let name: String = row.get(0);
        let scalar_type = ScalarType::from_dtype_index(row.get::<_, i32>(1) as u8)?;
        // TODO can I get a slice from psql driver?
        let shape = Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(2))?;
        let ret = ChConf::new(backend, series, scalar_type, shape, name);
        Ok(ret)
    }
}
