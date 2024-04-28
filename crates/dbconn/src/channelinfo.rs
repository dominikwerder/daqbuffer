use err::thiserror;
use err::ThisError;
use netpod::ScalarType;
use netpod::Shape;
use tokio_postgres::Client;

#[derive(Debug, ThisError)]
pub enum Error {
    Pg(#[from] crate::pg::Error),
    BadValue,
}

pub struct ChannelInfo {
    pub series: u64,
    pub backend: String,
    pub name: String,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub kind: u16,
}

pub async fn info_for_series_ids(series_ids: &[u64], pg: &Client) -> Result<Vec<ChannelInfo>, Error> {
    let (ord, seriess) = series_ids
        .iter()
        .enumerate()
        .fold((Vec::new(), Vec::new()), |mut a, x| {
            a.0.push(x.0 as i32);
            a.1.push(*x.1 as i64);
            a
        });
    let sql = concat!(
        "with q1 as (",
        " select * from unnest($1, $2) as inp (ord, series)",
        ")",
        "select q1.ord, q1.series, t.facility, t.channel, t.scalar_type, t.shape_dims, t.kind",
        " from q1",
        " join series_by_channel t on t.series = q1.series",
        " order by q1.ord",
    );
    use crate::pg::Type;
    let st = pg.prepare_typed(sql, &[Type::INT4_ARRAY, Type::INT8_ARRAY]).await?;
    let rows = pg.query(&st, &[&ord, &seriess]).await?;
    let mut ret = Vec::new();
    for row in rows {
        let series: i64 = row.get(1);
        let backend: String = row.get(2);
        let channel: String = row.get(3);
        let scalar_type: i32 = row.get(4);
        let shape_dims: Vec<i32> = row.get(5);
        let kind: i16 = row.get(6);
        let series = series as u64;
        let scalar_type = ScalarType::from_scylla_i32(scalar_type).map_err(|_| Error::BadValue)?;
        let shape = Shape::from_scylla_shape_dims(&shape_dims).map_err(|_| Error::BadValue)?;
        let kind = kind as u16;
        let e = ChannelInfo {
            series,
            backend,
            name: channel,
            scalar_type,
            shape,
            kind,
        };
        ret.push(e);
    }
    Ok(ret)
}
