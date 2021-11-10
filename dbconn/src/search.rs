use crate::create_connection;
use err::Error;
use netpod::{ChannelArchiver, ChannelSearchQuery, ChannelSearchResult, ChannelSearchSingleResult, NodeConfigCached};
use serde_json::Value as JsVal;

pub async fn search_channel_databuffer(
    query: ChannelSearchQuery,
    node_config: &NodeConfigCached,
) -> Result<ChannelSearchResult, Error> {
    let sql = format!(concat!(
        "select ",
        "channel_id, channel_name, source_name, dtype, shape, unit, description, channel_backend",
        " from searchext($1, $2, $3, $4)",
    ));
    let cl = create_connection(&node_config.node_config.cluster.database).await?;
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
        let ty: String = row.get(3);
        let k = ChannelSearchSingleResult {
            backend: row.get(7),
            name: row.get(1),
            source: row.get(2),
            ty,
            shape: shape,
            unit: row.get(5),
            description: row.get(6),
            is_api_0: None,
        };
        res.push(k);
    }
    let ret = ChannelSearchResult { channels: res };
    Ok(ret)
}

pub async fn search_channel_archeng(
    query: ChannelSearchQuery,
    backend: String,
    conf: &ChannelArchiver,
) -> Result<ChannelSearchResult, Error> {
    let sql = format!(concat!(
        "select c.name, c.config",
        " from channels c",
        " where c.name ~* $1",
        " order by c.name",
        " limit 100"
    ));
    let cl = create_connection(&conf.database).await?;
    let rows = cl.query(sql.as_str(), &[&query.name_regex]).await?;
    let mut res = vec![];
    for row in rows {
        let name: String = row.get(0);
        let config: JsVal = row.get(1);
        let st = match config.get("scalarType") {
            Some(k) => match k {
                JsVal::String(k) => match k.as_str() {
                    "U8" => "Uint8",
                    "U16" => "Uint16",
                    "U32" => "Uint32",
                    "U64" => "Uint64",
                    "I8" => "Int8",
                    "I16" => "Int16",
                    "I32" => "Int32",
                    "I64" => "Int64",
                    "F32" => "Float32",
                    "F64" => "Float64",
                    _ => k,
                }
                .into(),
                _ => "",
            },
            None => "",
        };
        let shape = match config.get("shape") {
            Some(k) => match k {
                JsVal::String(k) => {
                    if k == "Scalar" {
                        vec![]
                    } else {
                        return Err(Error::with_msg_no_trace(format!("search_channel_archeng can not understand {:?}", config)));
                    }
                }
                JsVal::Object(k) => match k.get("Wave") {
                    Some(k) => match k {
                        JsVal::Number(k) => {
                            vec![k.as_i64().unwrap_or(u32::MAX as i64) as u32]
                        }
                        _ => {
                            return Err(Error::with_msg_no_trace(format!("search_channel_archeng can not understand {:?}", config)));
                        }
                    },
                    None => {
                        return Err(Error::with_msg_no_trace(format!("search_channel_archeng can not understand {:?}", config)));
                    }
                },
                _ => {
                    return Err(Error::with_msg_no_trace(format!("search_channel_archeng can not understand {:?}", config)));
                }
            },
            None => vec![],
        };
        let k = ChannelSearchSingleResult {
            backend: backend.clone(),
            name,
            source: String::new(),
            ty: st.into(),
            shape,
            unit: String::new(),
            description: String::new(),
            is_api_0: None,
        };
        res.push(k);
    }
    let ret = ChannelSearchResult { channels: res };
    Ok(ret)
}

pub async fn search_channel(
    query: ChannelSearchQuery,
    node_config: &NodeConfigCached,
) -> Result<ChannelSearchResult, Error> {
    if let Some(conf) = node_config.node.channel_archiver.as_ref() {
        search_channel_archeng(query, node_config.node.backend.clone(), conf).await
    } else if let Some(_conf) = node_config.node.archiver_appliance.as_ref() {
        err::todoval()
    } else {
        search_channel_databuffer(query, node_config).await
    }
}
