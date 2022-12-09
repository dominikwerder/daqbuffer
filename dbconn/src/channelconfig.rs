use err::Error;
use netpod::log::*;
use netpod::{Channel, NodeConfigCached, ScalarType, Shape};

use crate::ErrConv;

pub struct ChConf {
    pub series: u64,
    pub scalar_type: ScalarType,
    pub shape: Shape,
}

/// It is an unsolved question as to how we want to uniquely address channels.
/// Currently, the usual (backend, channelname) works in 99% of the cases, but the edge-cases
/// are not solved. At the same time, it is desirable to avoid to complicate things for users.
/// Current state:
/// If the series id is given, we take that.
/// Otherwise we try to uniquely identify the series id from the given information.
/// In the future, we can even try to involve time range information for that, but backends like
/// old archivers and sf databuffer do not support such lookup.
pub async fn chconf_from_database(channel: &Channel, ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    if channel.backend != ncc.node_config.cluster.backend {
        warn!(
            "mismatched backend  {}  vs  {}",
            channel.backend, ncc.node_config.cluster.backend
        );
    }
    if channel.backend() == "test-inmem" {
        let ret = if channel.name() == "inmem-d0-i32" {
            let ret = ChConf {
                series: 1,
                scalar_type: ScalarType::I32,
                shape: Shape::Scalar,
            };
            Ok(ret)
        } else {
            error!("no test information");
            Err(Error::with_msg_no_trace(format!("no test information"))
                .add_public_msg("No channel config for test channel {:?}"))
        };
        return ret;
    }
    if channel.backend() == "test-disk-databuffer" {
        // TODO the series-ids here are just random. Need to integrate with better test setup.
        let ret = if channel.name() == "scalar-i32-be" {
            let ret = ChConf {
                series: 1,
                scalar_type: ScalarType::I32,
                shape: Shape::Scalar,
            };
            Ok(ret)
        } else if channel.name() == "wave-f64-be-n21" {
            let ret = ChConf {
                series: 2,
                scalar_type: ScalarType::F64,
                shape: Shape::Wave(21),
            };
            Ok(ret)
        } else if channel.name() == "const-regular-scalar-i32-be" {
            let ret = ChConf {
                series: 3,
                scalar_type: ScalarType::I32,
                shape: Shape::Scalar,
            };
            Ok(ret)
        } else {
            error!("no test information");
            Err(Error::with_msg_no_trace(format!("no test information"))
                .add_public_msg("No channel config for test channel {:?}"))
        };
        return ret;
    }
    // TODO use a common already running worker pool for these queries:
    let dbconf = &ncc.node_config.cluster.database;
    let dburl = format!(
        "postgresql://{}:{}@{}:{}/{}",
        dbconf.user, dbconf.pass, dbconf.host, dbconf.port, dbconf.name
    );
    let (pgclient, pgconn) = tokio_postgres::connect(&dburl, tokio_postgres::NoTls)
        .await
        .err_conv()?;
    tokio::spawn(pgconn);
    if let Some(series) = channel.series() {
        let res = pgclient
            .query(
                "select scalar_type, shape_dims from series_by_channel where series = $1",
                &[&(series as i64)],
            )
            .await
            .err_conv()?;
        if res.len() < 1 {
            warn!("can not find channel information for series {series} given through {channel:?}");
            let e = Error::with_public_msg_no_trace(format!("can not find channel information for {channel:?}"));
            Err(e)
        } else {
            let row = res.first().unwrap();
            let scalar_type = ScalarType::from_dtype_index(row.get::<_, i32>(0) as u8)?;
            // TODO can I get a slice from psql driver?
            let shape = Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(1))?;
            let ret = ChConf {
                series,
                scalar_type,
                shape,
            };
            Ok(ret)
        }
    } else {
        let res = pgclient
            .query(
                "select series, scalar_type, shape_dims from series_by_channel where facility = $1 and channel = $2",
                &[&channel.backend(), &channel.name()],
            )
            .await
            .err_conv()?;
        if res.len() < 1 {
            warn!("can not find channel information for {channel:?}");
            let e = Error::with_public_msg_no_trace(format!("can not find channel information for {channel:?}"));
            Err(e)
        } else if res.len() > 1 {
            warn!("ambigious channel {channel:?}");
            let e = Error::with_public_msg_no_trace(format!("ambigious channel {channel:?}"));
            Err(e)
        } else {
            let row = res.first().unwrap();
            let series = row.get::<_, i64>(0) as u64;
            let scalar_type = ScalarType::from_dtype_index(row.get::<_, i32>(1) as u8)?;
            // TODO can I get a slice from psql driver?
            let shape = Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(2))?;
            let ret = ChConf {
                series,
                scalar_type,
                shape,
            };
            Ok(ret)
        }
    }
}
