use err::Error;
use netpod::log::*;
use std::convert::TryInto;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

// TODO
// Wrap each I/O operation into a stopwatch to detect bad NFS performance!

#[derive(Debug)]
pub struct IndexFileBasics {
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

impl IndexFileBasics {
    pub fn file_offset_size(&self) -> u64 {
        if self.version == 3 {
            64
        } else if self.version == 2 {
            32
        } else {
            panic!()
        }
    }
}

pub fn name_hash(s: &str, ht_len: u32) -> u32 {
    let mut h = 0;
    for ch in s.as_bytes() {
        h = (128 * h + *ch as u32) % ht_len;
    }
    h
}

pub async fn read_file_basics(path: PathBuf) -> Result<IndexFileBasics, Error> {
    let mut f1 = OpenOptions::new().read(true).open(path).await?;
    let mut buf = vec![0; 0x4000];
    f1.read_exact(&mut buf).await?;
    let version = String::from_utf8(buf[3..4].to_vec())?.parse()?;
    fn readu64(buf: &[u8], pos: usize) -> u64 {
        u64::from_be_bytes(buf.as_ref()[pos..pos + 8].try_into().unwrap()) as u64
    }
    fn readu32(buf: &[u8], pos: usize) -> u64 {
        u32::from_be_bytes(buf.as_ref()[pos..pos + 4].try_into().unwrap()) as u64
    }
    let b = &buf;
    //let s: String = b.iter().map(|x| format!(" {:02x}", *x)).collect();
    //info!("\n\n{}", s);
    let mut i1 = 0x2800;
    while i1 < 0x2880 {
        let s: String = b[i1..i1 + 8].iter().map(|x| format!(" {:02x}", *x)).collect();
        info!("{}", s);
        i1 += 8;
    }
    info!("{}", String::from_utf8_lossy(&b[0x2800..0x2880]));
    let ret = IndexFileBasics {
        version,
        name_hash_anchor_beg: readu64(b, 0x04),
        name_hash_anchor_len: readu32(b, 0x0c),
        fa_used_list_len: readu64(b, 0x10),
        fa_used_list_beg: readu64(b, 0x18),
        fa_used_list_end: readu64(b, 0x20),
        fa_free_list_len: readu64(b, 0x28),
        fa_free_list_beg: readu64(b, 0x30),
        fa_free_list_end: readu64(b, 0x38),
        fa_header_len: readu64(b, 0x40),
        fa_header_prev: readu64(b, 0x48),
        fa_header_next: readu64(b, 0x50),
    };
    let chn_hash = name_hash("X05DA-FE-WI1:TC1", ret.name_hash_anchor_len as u32);
    info!("channel hash: {:08x}", chn_hash);
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
            assert_eq!(res.version, 3);
            assert_eq!(res.file_offset_size(), 64);
            Ok(())
        };
        Ok(taskrun::run(fut).unwrap())
    }
}
