use crate::unescape_archapp_msg;
use err::Error;
use netpod::log::*;
use protobuf::Message;

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
