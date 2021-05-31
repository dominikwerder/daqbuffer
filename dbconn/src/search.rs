use crate::create_connection;
use err::Error;
use netpod::{ChannelSearchQuery, ChannelSearchResult, ChannelSearchSingleResult, NodeConfigCached};

pub async fn search_channel(
    query: ChannelSearchQuery,
    node_config: &NodeConfigCached,
) -> Result<ChannelSearchResult, Error> {
    let sql = format!(concat!(
        "select ",
        "channel_id, channel_name, source_name, dtype, shape, unit, description, channel_backend",
        " from searchext($1, $2, $3, $4)",
    ));
    let cl = create_connection(node_config).await?;
    let rows = cl
        .query(
            sql.as_str(),
            &[&query.name_regex, &query.source_regex, &query.description_regex, &"asc"],
        )
        .await?;
    let mut res = vec![];
    for row in rows {
        let shapedb: Option<serde_json::Value> = row.get(4);
        let shape = match &shapedb {
            Some(top) => match top {
                serde_json::Value::Null => vec![],
                serde_json::Value::Array(items) => {
                    let mut a = vec![];
                    for item in items {
                        match item {
                            serde_json::Value::Number(n) => match n.as_i64() {
                                Some(n) => {
                                    a.push(n as u32);
                                }
                                None => return Err(Error::with_msg(format!("can not understand shape {:?}", shapedb))),
                            },
                            _ => return Err(Error::with_msg(format!("can not understand shape {:?}", shapedb))),
                        }
                    }
                    a
                }
                _ => return Err(Error::with_msg(format!("can not understand shape {:?}", shapedb))),
            },
            None => vec![],
        };
        let k = ChannelSearchSingleResult {
            backend: row.get(7),
            name: row.get(1),
            source: row.get(2),
            ty: row.get(3),
            shape: shape,
            unit: row.get(5),
            description: row.get(6),
        };
        res.push(k);
    }
    let ret = ChannelSearchResult { channels: res };
    Ok(ret)
}
