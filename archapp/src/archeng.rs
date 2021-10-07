use err::Error;
use std::convert::TryInto;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

// TODO
// Wrap each I/O operation into a stopwatch to detect bad NFS performance!

#[derive(Debug)]
struct IndexFileBasics {
    version: u8,
    name_hash_anchor_beg: u64,
    name_hash_anchor_len: u64,
    fa_used_list_beg: u64,
    fa_used_list_end: u64,
    fa_used_list_len: u64,
    fa_free_list_beg: u64,
    fa_free_list_end: u64,
    fa_free_list_len: u64,
    fa_header_prev: u64,
    fa_header_next: u64,
    fa_header_len: u64,
}

pub async fn read_file_basics(path: PathBuf) -> Result<IndexFileBasics, Error> {
    let mut f1 = OpenOptions::new().read(true).open(path).await?;
    let mut buf = vec![0; 0x34];
    f1.read_exact(&mut buf).await?;
    let version = String::from_utf8(buf[3..4].to_vec())?.parse()?;
    let ret = IndexFileBasics {
        version,
        name_hash_anchor_beg: u32::from_be_bytes(buf[0x04..0x08].try_into().unwrap()) as u64,
        name_hash_anchor_len: u32::from_be_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        // TODO:
        fa_used_list_beg: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_used_list_end: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_used_list_len: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_free_list_beg: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_free_list_end: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_free_list_len: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_header_prev: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_header_next: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
        fa_header_len: u32::from_le_bytes(buf[0x08..0x0c].try_into().unwrap()) as u64,
    };
    Ok(ret)
}

#[cfg(test)]
mod test {
    // TODO move RangeFilter to a different crate (items?)
    // because the `disk` crate should become the specific sf-databuffer reader engine.

    //use disk::rangefilter::RangeFilter;
    //use disk::{eventblobs::EventChunkerMultifile, eventchunker::EventChunkerConf};

    use crate::archeng::read_file_basics;
    use err::Error;
    use futures_util::StreamExt;
    use items::{RangeCompletableItem, StreamItem};
    use netpod::log::*;
    use netpod::timeunits::{DAY, MS};
    use netpod::{ByteSize, ChannelConfig, FileIoBufferSize, Nanos};
    use std::path::PathBuf;

    fn open_index_inner(path: impl Into<PathBuf>) -> Result<(), Error> {
        let task = async move { Ok(()) };
        Ok(taskrun::run(task).unwrap())
    }

    const CHN_0_MASTER_INDEX: &str = "/data/daqbuffer-testdata/sls/gfa03/bl_arch/archive_X05DA_SH/index";

    #[test]
    fn read_magic() -> Result<(), Error> {
        let fut = async {
            let res = read_file_basics(CHN_0_MASTER_INDEX.into()).await?;
            info!("got {:?}", res);
            assert!(res.version == 2 || res.version == 3);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }
}
