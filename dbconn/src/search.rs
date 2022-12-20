use crate::create_connection;
use crate::ErrConv;
use err::Error;
use netpod::ChannelArchiver;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::ChannelSearchSingleResult;
use netpod::Database;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::ScyllaConfig;
use netpod::Shape;
use serde_json::Value as JsVal;

pub async fn search_channel_databuffer(
    query: ChannelSearchQuery,
    node_config: &NodeConfigCached,
) -> Result<ChannelSearchResult, Error> {
    let empty = if !query.name_regex.is_empty() {
        false
    } else if !query.source_regex.is_empty() {
        false
    } else if !query.description_regex.is_empty() {
        false
    } else {
        true
    };
    if empty {
        let ret = ChannelSearchResult { channels: Vec::new() };
        return Ok(ret);
    }
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
        .await
        .err_conv()?;
    let mut res = Vec::new();
    for row in rows {
        let shapedb: Option<serde_json::Value> = row.get(4);
        let shape = match &shapedb {
            Some(top) => match top {
                serde_json::Value::Null => Vec::new(),
                serde_json::Value::Array(items) => {
                    let mut a = Vec::new();
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
            None => Vec::new(),
        };
        let ty: String = row.get(3);
        let k = ChannelSearchSingleResult {
            backend: row.get(7),
            name: row.get(1),
            series: row.get::<_, i64>(0) as u64,
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

pub async fn search_channel_scylla(
    query: ChannelSearchQuery,
    _scyconf: &ScyllaConfig,
    pgconf: &Database,
) -> Result<ChannelSearchResult, Error> {
    let empty = if !query.name_regex.is_empty() { false } else { true };
    if empty {
        let ret = ChannelSearchResult { channels: Vec::new() };
        return Ok(ret);
    }
    let sql = format!(concat!(
        "select",
        " series, facility, channel, scalar_type, shape_dims",
        " from series_by_channel",
        " where channel ~* $1",
        " limit 400000",
    ));
    let pgclient = crate::create_connection(pgconf).await?;
    let rows = pgclient.query(sql.as_str(), &[&query.name_regex]).await.err_conv()?;
    let mut res = Vec::new();
    for row in rows {
        let series: i64 = row.get(0);
        let series = series as u64;
        let backend: String = row.get(1);
        let channel: String = row.get(2);
        let a: i32 = row.get(3);
        let scalar_type = ScalarType::from_scylla_i32(a)?;
        let a: Vec<i32> = row.get(4);
        let shape = Shape::from_scylla_shape_dims(&a)?;
        let k = ChannelSearchSingleResult {
            backend,
            name: channel,
            series,
            source: "".into(),
            ty: scalar_type.to_variant_str().into(),
            shape: shape.to_scylla_vec().into_iter().map(|x| x as u32).collect(),
            unit: "".into(),
            description: "".into(),
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
    _conf: &ChannelArchiver,
    database: &Database,
) -> Result<ChannelSearchResult, Error> {
    // Channel archiver provides only channel name. Also, search criteria are currently ANDed.
    // Therefore search only if user only provides a name criterion.
    let empty = if !query.source_regex.is_empty() {
        true
    } else if !query.description_regex.is_empty() {
        true
    } else if query.name_regex.is_empty() {
        true
    } else {
        false
    };
    if empty {
        let ret = ChannelSearchResult { channels: Vec::new() };
        return Ok(ret);
    }
    let sql = format!(concat!(
        "select c.name, c.config",
        " from channels c",
        " where c.name ~* $1",
        " order by c.name",
        " limit 100"
    ));
    let cl = create_connection(database).await?;
    let rows = cl.query(sql.as_str(), &[&query.name_regex]).await.err_conv()?;
    let mut res = Vec::new();
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
                        Vec::new()
                    } else {
                        return Err(Error::with_msg_no_trace(format!(
                            "search_channel_archeng can not understand {:?}",
                            config
                        )));
                    }
                }
                JsVal::Object(k) => match k.get("Wave") {
                    Some(k) => match k {
                        JsVal::Number(k) => {
                            vec![k.as_i64().unwrap_or(u32::MAX as i64) as u32]
                        }
                        _ => {
                            return Err(Error::with_msg_no_trace(format!(
                                "search_channel_archeng can not understand {:?}",
                                config
                            )));
                        }
                    },
                    None => {
                        return Err(Error::with_msg_no_trace(format!(
                            "search_channel_archeng can not understand {:?}",
                            config
                        )));
                    }
                },
                _ => {
                    return Err(Error::with_msg_no_trace(format!(
                        "search_channel_archeng can not understand {:?}",
                        config
                    )));
                }
            },
            None => Vec::new(),
        };
        let k = ChannelSearchSingleResult {
            backend: backend.clone(),
            name,
            // TODO provide a unique id also within this backend:
            series: 0,
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
    let pgconf = &node_config.node_config.cluster.database;
    if let Some(scyconf) = node_config.node_config.cluster.scylla.as_ref() {
        search_channel_scylla(query, scyconf, pgconf).await
    } else if let Some(conf) = node_config.node.channel_archiver.as_ref() {
        search_channel_archeng(query, node_config.node_config.cluster.backend.clone(), conf, pgconf).await
    } else if let Some(_conf) = node_config.node.archiver_appliance.as_ref() {
        // TODO
        err::todoval()
    } else {
        search_channel_databuffer(query, node_config).await
    }
}
