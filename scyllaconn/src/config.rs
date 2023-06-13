use crate::errconv::ErrConv;
use err::Error;
use futures_util::StreamExt;
use netpod::{log::*, ScalarType, SfDbChannel, Shape};
use netpod::{ChannelConfigQuery, ChannelConfigResponse};
use scylla::Session as ScySession;
use std::sync::Arc;

// TODO unused, table in postgres.
pub async fn config_from_scylla(chq: ChannelConfigQuery, scy: Arc<ScySession>) -> Result<ChannelConfigResponse, Error> {
    let cql = "select series, scalar_type, shape_dims from series_by_channel where facility = ? and channel_name = ?";
    let mut it = scy
        .query_iter(cql, (chq.channel.backend(), chq.channel.name()))
        .await
        .err_conv()?;
    let mut rows = Vec::new();
    while let Some(row) = it.next().await {
        let row = row.err_conv()?;
        let cols = row.into_typed::<(i64, i32, Vec<i32>)>().err_conv()?;
        let scalar_type = ScalarType::from_scylla_i32(cols.1)?;
        let shape = Shape::from_scylla_shape_dims(&cols.2)?;
        let channel = SfDbChannel {
            series: Some(cols.0 as _),
            backend: chq.channel.backend().into(),
            name: chq.channel.name().into(),
        };
        let res = ChannelConfigResponse {
            channel,
            scalar_type,
            byte_order: None,
            shape,
        };
        info!("config_from_scylla: {res:?}");
        rows.push(res);
    }
    if rows.is_empty() {
        return Err(Error::with_public_msg_no_trace(format!(
            "can not find config for channel {:?}",
            chq.channel
        )));
    } else {
        if rows.len() > 1 {
            error!(
                "Found multiple configurations for channel  {:?}  {:?}",
                chq.channel, rows
            );
        }
        Ok(rows.pop().unwrap())
    }
}
