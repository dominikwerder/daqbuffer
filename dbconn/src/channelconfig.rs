use crate::ErrConv;
use err::Error;
use netpod::log::*;
use netpod::ChConf;
use netpod::Channel;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::Shape;

/// It is an unsolved question as to how we want to uniquely address channels.
/// Currently, the usual (backend, channelname) works in 99% of the cases, but the edge-cases
/// are not solved. At the same time, it is desirable to avoid to complicate things for users.
/// Current state:
/// If the series id is given, we take that.
/// Otherwise we try to uniquely identify the series id from the given information.
/// In the future, we can even try to involve time range information for that, but backends like
/// old archivers and sf databuffer do not support such lookup.
pub async fn chconf_from_scylla_type_backend(channel: &Channel, ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    if channel.backend != ncc.node_config.cluster.backend {
        warn!(
            "mismatched backend  {}  vs  {}",
            channel.backend, ncc.node_config.cluster.backend
        );
    }
    let backend = channel.backend().into();
    let dbconf = &ncc.node_config.cluster.database;
    let pgclient = crate::create_connection(dbconf).await?;
    if let Some(series) = channel.series() {
        let res = pgclient
            .query(
                "select channel, scalar_type, shape_dims from series_by_channel where facility = $1 and series = $2",
                &[&channel.backend(), &(series as i64)],
            )
            .await
            .err_conv()?;
        if res.len() < 1 {
            warn!("can not find channel information for series {series} given through {channel:?}");
            let e = Error::with_public_msg_no_trace(format!("can not find channel information for {channel:?}"));
            Err(e)
        } else {
            let row = res.first().unwrap();
            let name: String = row.get(0);
            let scalar_type = ScalarType::from_dtype_index(row.get::<_, i32>(1) as u8)?;
            // TODO can I get a slice from psql driver?
            let shape = Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(2))?;
            let ret = ChConf {
                backend,
                series: Some(series),
                name,
                scalar_type,
                shape,
            };
            Ok(ret)
        }
    } else {
        if ncc.node_config.cluster.scylla.is_some() {
            let res = pgclient
            .query(
                "select channel, series, scalar_type, shape_dims from series_by_channel where facility = $1 and channel = $2",
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
                let name: String = row.get(0);
                let series = row.get::<_, i64>(1) as u64;
                let scalar_type = ScalarType::from_dtype_index(row.get::<_, i32>(2) as u8)?;
                // TODO can I get a slice from psql driver?
                let shape = Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(3))?;
                let ret = ChConf {
                    backend,
                    series: Some(series),
                    name,
                    scalar_type,
                    shape,
                };
                Ok(ret)
            }
        } else {
            error!("TODO xm89ur8932cr");
            Err(Error::with_msg_no_trace("TODO xm89ur8932cr"))
        }
    }
}
