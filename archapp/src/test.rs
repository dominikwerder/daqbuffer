use err::Error;
use netpod::log::*;
use protobuf::Message;

fn unescape_archapp_msg(inp: &[u8]) -> Result<Vec<u8>, Error> {
    let mut ret = Vec::with_capacity(inp.len() * 5 / 4);
    let mut esc = false;
    for &k in inp.iter() {
        if k == 0x1b {
            esc = true;
        } else if esc {
            if k == 0x1 {
                ret.push(0x1b);
            } else if k == 0x2 {
                ret.push(0xa);
            } else if k == 0x3 {
                ret.push(0xd);
            } else {
                return Err(Error::with_msg("malformed escaped archapp message"));
            }
            esc = false;
        } else {
            ret.push(k);
        }
    }
    Ok(ret)
}

#[test]
fn read_pb_00() -> Result<(), Error> {
    let block1 = async move {
        let path = "../../../../archappdata/tmp/lts/ArchiverStore/SARUN16/MQUA080/X:2021_01.pb";
        let f1 = tokio::fs::read(path).await?;
        let mut j1 = 0;
        loop {
            let mut i2 = usize::MAX;
            for (i1, &k) in f1[j1..].iter().enumerate() {
                if k == 0xa {
                    i2 = j1 + i1;
                    break;
                }
            }
            if i2 != usize::MAX {
                info!("got NL  {} .. {}", j1, i2);
                let m = unescape_archapp_msg(&f1[j1..i2])?;
                if j1 == 0 {
                    let payload_info = crate::generated::EPICSEvent::PayloadInfo::parse_from_bytes(&m).unwrap();
                    info!("got payload_info: {:?}", payload_info);
                } else {
                    let scalar_double = crate::generated::EPICSEvent::ScalarDouble::parse_from_bytes(&m).unwrap();
                    info!("got scalar_double: {:?}", scalar_double);
                }
            } else {
                info!("no more packets");
                break;
            }
            j1 = i2 + 1;
        }
        Ok::<_, Error>(())
    };
    taskrun::run(block1)?;
    Ok(())
}
