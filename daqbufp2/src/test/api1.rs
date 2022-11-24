#[cfg(test)]
mod api1_parse;

use crate::nodes::require_test_hosts_running;
use crate::test::api1::api1_parse::Api1Frame;
use err::Error;
use futures_util::Future;
use httpclient::http_post;
use httpret::api1::Api1ScalarType;
use netpod::log::*;
use netpod::query::api1::{Api1Query, Api1Range, ChannelTuple};
use std::fmt;
use url::Url;

fn testrun<T, F>(fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    taskrun::run(fut)
}

fn is_monitonic_strict<I>(it: I) -> bool
where
    I: Iterator,
    <I as Iterator>::Item: PartialOrd + fmt::Debug,
{
    let mut last = None;
    for x in it {
        if let Some(last) = last.as_ref() {
            if x <= *last {
                return false;
            }
        }
        last = Some(x);
    }
    true
}

#[test]
fn test_is_monitonic_strict() {
    assert_eq!(is_monitonic_strict([1, 2, 3].iter()), true);
    assert_eq!(is_monitonic_strict([1, 2, 2].iter()), false);
}

#[test]
fn events_f64_plain() -> Result<(), Error> {
    let fut = async {
        let rh = require_test_hosts_running()?;
        let cluster = &rh.cluster;
        let node = &cluster.nodes[0];
        let url: Url = format!("http://{}:{}/api/1/query", node.host, node.port).parse()?;
        let accept = "application/octet-stream";
        let range = Api1Range::new("1970-01-01T00:00:00Z".try_into()?, "1970-01-01T00:01:00Z".try_into()?)?;
        // TODO the channel list needs to get pre-processed to check for backend prefix!
        let ch = ChannelTuple::new("test-disk-databuffer".into(), "scalar-i32-be".into());
        let qu = Api1Query::new(range, vec![ch]);
        let body = serde_json::to_string(&qu)?;
        let buf = http_post(url, accept, body.into()).await?;
        eprintln!("body received: {}", buf.len());
        match api1_parse::api1_frames(&buf) {
            Ok((_, frames)) => {
                debug!("FRAMES LEN: {}", frames.len());
                assert_eq!(frames.len(), 121);
                if let Api1Frame::Header(header) = &frames[0] {
                    if let Api1ScalarType::I32 = header.header().ty() {
                    } else {
                        panic!("bad scalar type")
                    }
                } else {
                    panic!("bad header")
                }
                let tss: Vec<_> = frames
                    .iter()
                    .filter_map(|f| match f {
                        api1_parse::Api1Frame::Data(d) => Some(d.ts()),
                        _ => None,
                    })
                    .collect();
                let pulses: Vec<_> = frames
                    .iter()
                    .filter_map(|f| match f {
                        api1_parse::Api1Frame::Data(d) => Some(d.pulse()),
                        _ => None,
                    })
                    .collect();
                let values: Vec<_> = frames
                    .iter()
                    .filter_map(|f| match f {
                        api1_parse::Api1Frame::Data(d) => {
                            let val = i32::from_be_bytes(d.data().try_into().unwrap());
                            Some(val)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(is_monitonic_strict(tss.iter()), true);
                assert_eq!(is_monitonic_strict(pulses.iter()), true);
                assert_eq!(is_monitonic_strict(values.iter()), true);
                for &val in &values {
                    assert!(val >= 0);
                    assert!(val < 120);
                }
            }
            Err(e) => {
                error!("can not parse result: {e}");
                panic!()
            }
        };
        Ok(())
    };
    testrun(fut)?;
    Ok(())
}
