#[allow(unused_imports)]
use tracing::{error, warn, info, debug, trace};
use err::Error;
use std::task::{Context, Poll};
use std::future::Future;
use futures_core::Stream;
use futures_util::future::FusedFuture;
use futures_util::{FutureExt, StreamExt, pin_mut, select};
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio::fs::{OpenOptions, File};
use bytes::{Bytes, BytesMut, Buf};
use std::path::PathBuf;
use bitshuffle::bitshuffle_decompress;
use netpod::{ScalarType, Shape, Node, ChannelConfig};
use std::sync::Arc;
use crate::dtflags::{COMPRESSION, BIG_ENDIAN, ARRAY, SHAPE};

pub mod agg;
pub mod gen;
pub mod merge;
pub mod cache;
pub mod raw;
pub mod channelconfig;


pub async fn read_test_1(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> Result<netpod::BodyStream, Error> {
    let path = datapath(query.timebin as u64, &query.channel_config, &node);
    debug!("try path: {:?}", path);
    let fin = OpenOptions::new()
        .read(true)
        .open(path)
        .await?;
    let meta = fin.metadata().await;
    debug!("file meta {:?}", meta);
    let stream = netpod::BodyStream {
        inner: Box::new(FileReader {
            file: fin,
            nreads: 0,
            buffer_size: query.buffer_size,
        }),
    };
    Ok(stream)
}

struct FileReader {
    file: tokio::fs::File,
    nreads: u32,
    buffer_size: u32,
}

impl Stream for FileReader {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let blen = self.buffer_size as usize;
        let mut buf2 = BytesMut::with_capacity(blen);
        buf2.resize(buf2.capacity(), 0);
        if buf2.as_mut().len() != blen {
            panic!("logic");
        }
        let mut buf = tokio::io::ReadBuf::new(buf2.as_mut());
        if buf.filled().len() != 0 {
            panic!("logic");
        }
        match Pin::new(&mut self.file).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(_)) => {
                let rlen = buf.filled().len();
                if rlen == 0 {
                    Poll::Ready(None)
                }
                else {
                    if rlen != blen {
                        info!("short read  {} of {}", buf.filled().len(), blen);
                    }
                    self.nreads += 1;
                    Poll::Ready(Some(Ok(buf2.freeze())))
                }
            }
            Poll::Ready(Err(e)) => {
                Poll::Ready(Some(Err(Error::from(e))))
            }
            Poll::Pending => Poll::Pending
        }
    }

}


#[allow(dead_code)]
struct Fopen1 {
    opts: OpenOptions,
    fut: Pin<Box<dyn Future<Output=Result<File, std::io::Error>>>>,
    term: bool,
}

impl Fopen1 {

    pub fn new(path: PathBuf) -> Self {
        let fut = Box::pin(async {
            let mut o1 = OpenOptions::new();
            let o2 = o1.read(true);
            let res = o2.open(path);
            //() == res;
            //todo!()
            res.await
        }) as Pin<Box<dyn Future<Output=Result<File, std::io::Error>>>>;
        let _fut2: Box<dyn Future<Output=u32>> = Box::new(async {
            123
        });
        Self {
            opts: OpenOptions::new(),
            fut,
            term: false,
        }
    }

}

impl Future for Fopen1 {
    type Output = Result<File, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let g = self.fut.as_mut();
        match g.poll(cx) {
            Poll::Ready(Ok(k)) => {
                self.term = true;
                Poll::Ready(Ok(k))
            },
            Poll::Ready(Err(k)) => {
                self.term = true;
                Poll::Ready(Err(k.into()))
            },
            Poll::Pending => Poll::Pending,
        }
    }

}


impl FusedFuture for Fopen1 {
    fn is_terminated(&self) -> bool {
        self.term
    }
}

unsafe impl Send for Fopen1 {}


pub fn raw_concat_channel_read_stream_try_open_in_background(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> impl Stream<Item=Result<Bytes, Error>> + Send {
    let mut query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        use tokio::io::AsyncReadExt;
        let mut fopen = None;
        let mut fopen_avail = false;
        let mut file_prep: Option<File> = None;
        let mut file: Option<File> = None;
        let mut reading = None;
        let mut i1 = 0;
        let mut i9 = 0;
        loop {
            let blen = query.buffer_size as usize;
            {
                if !fopen_avail && file_prep.is_none() && i1 < 16 {
                    info!("Prepare open task for next file {}", query.timebin + i1);
                    fopen.replace(Fopen1::new(datapath(query.timebin as u64 + i1 as u64, &query.channel_config, &node)));
                    fopen_avail = true;
                    i1 += 1;
                }
            }
            if !fopen_avail && file_prep.is_none() && file.is_none() && reading.is_none() {
                info!("Nothing more to do");
                break;
            }
            // TODO
            // When the file is available, I can prepare the next reading.
            // But next iteration, the file is not available, but reading is, so I should read!
            // I can not simply drop the reading future, that would lose the request.

            if reading.is_some() {
                let k: Result<(tokio::fs::File, BytesMut), Error> = reading.as_mut().unwrap().await;
                if k.is_err() {
                    error!("LONELY READ ERROR");
                }
                let k = k.unwrap();
                reading.take();
                file = Some(k.0);
                yield Ok(k.1.freeze());
            }
            else if fopen.is_some() {
                if file.is_some() {
                    if reading.is_none() {
                        let mut buf = BytesMut::with_capacity(blen);
                        let mut file2 = file.take().unwrap();
                        let a = async move {
                            file2.read_buf(&mut buf).await?;
                            Ok::<_, Error>((file2, buf))
                        };
                        let a = Box::pin(a);
                        reading = Some(a.fuse());
                    }
                    // TODO do I really have to take out the future while waiting on it?
                    // I think the issue is now with the mutex guard, can I get rid of the mutex again?
                    let mut fopen3 = fopen.take().unwrap();
                    let bufres = select! {
                        // TODO can I avoid the unwraps via matching already above?
                        f = fopen3 => {
                            fopen_avail = false;
                            // TODO feed out the potential error:
                            file_prep = Some(f.unwrap());
                            None
                        }
                        k = reading.as_mut().unwrap() => {
                            info!("COMBI  read chunk");
                            reading = None;
                            // TODO handle the error somehow here...
                            if k.is_err() {
                                error!("READ ERROR IN COMBI");
                            }
                            let k = k.unwrap();
                            file = Some(k.0);
                            Some(k.1)
                        }
                    };
                    if fopen_avail {
                        fopen.replace(fopen3);
                    }
                    if let Some(k) = bufres {
                        yield Ok(k.freeze());
                    }
                }
                else {
                    info!("-----------------   no file open yet, await only opening of the file");
                    // TODO try to avoid this duplicated code:
                    if fopen.is_none() {
                        error!("logic  BB");
                    }
                    let fopen3 = fopen.take().unwrap();
                    let f = fopen3.await?;
                    info!("opened next file SOLO");
                    fopen_avail = false;
                    file = Some(f);
                }
            }
            else if file.is_some() {
                loop {
                    let mut buf = BytesMut::with_capacity(blen);
                    let mut file2 = file.take().unwrap();
                    let n1 = file2.read_buf(&mut buf).await?;
                    if n1 == 0 {
                        if file_prep.is_some() {
                            file.replace(file_prep.take().unwrap());
                        }
                        else {
                            info!("After read loop, next file not yet ready");
                        }
                        break;
                    }
                    else {
                        file.replace(file2);
                        yield Ok(buf.freeze());
                    }
                }
            }
            i9 += 1;
            if i9 > 100 {
                break;
            }
        }
    }
}


pub fn raw_concat_channel_read_stream_file_pipe(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> impl Stream<Item=Result<BytesMut, Error>> + Send {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let chrx = open_files(&query, node.clone());
        while let Ok(file) = chrx.recv().await {
            let mut file = match file {
                Ok(k) => k,
                Err(_) => break
            };
            loop {
                let mut buf = BytesMut::with_capacity(query.buffer_size as usize);
                use tokio::io::AsyncReadExt;
                let n1 = file.read_buf(&mut buf).await?;
                if n1 == 0 {
                    info!("file EOF");
                    break;
                }
                else {
                    yield Ok(buf);
                }
            }
        }
    }
}

fn open_files(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> async_channel::Receiver<Result<tokio::fs::File, Error>> {
    let (chtx, chrx) = async_channel::bounded(2);
    let mut query = query.clone();
    let node = node.clone();
    tokio::spawn(async move {
        let tb0 = query.timebin;
        for i1 in 0..query.tb_file_count {
            query.timebin = tb0 + i1;
            let path = datapath(query.timebin as u64, &query.channel_config, &node);
            let fileres = OpenOptions::new()
                .read(true)
                .open(&path)
                .await;
            info!("opened file {:?}  {:?}", &path, &fileres);
            match fileres {
                Ok(k) => {
                    match chtx.send(Ok(k)).await {
                        Ok(_) => (),
                        Err(_) => break
                    }
                }
                Err(e) => {
                    match chtx.send(Err(e.into())).await {
                        Ok(_) => (),
                        Err(_) => break
                    }
                }
            }
        }
    });
    chrx
}


pub fn file_content_stream(mut file: tokio::fs::File, buffer_size: usize) -> impl Stream<Item=Result<BytesMut, Error>> + Send {
    async_stream::stream! {
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = BytesMut::with_capacity(buffer_size);
            let n1 = file.read_buf(&mut buf).await?;
            if n1 == 0 {
                info!("file EOF");
                break;
            }
            else {
                yield Ok(buf);
            }
        }
    }
}


pub fn parsed1(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> impl Stream<Item=Result<Bytes, Error>> + Send {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let filerx = open_files(&query, node.clone());
        while let Ok(fileres) = filerx.recv().await {
            match fileres {
                Ok(file) => {
                    let inp = Box::pin(file_content_stream(file, query.buffer_size as usize));
                    let mut chunker = EventChunker::new(inp, todo!());
                    while let Some(evres) = chunker.next().await {
                        match evres {
                            Ok(evres) => {
                                //let mut buf = BytesMut::with_capacity(16);
                                // TODO put some interesting information to test
                                //buf.put_u64_le(0xcafecafe);
                                //yield Ok(buf.freeze())
                                for bufopt in evres.decomps {
                                    if let Some(buf) = bufopt {
                                        yield Ok(buf.freeze());
                                    }
                                }
                            }
                            Err(e) => {
                                yield Err(e)
                            }
                        }
                    }
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    }
}


pub struct EventBlobsComplete {
    channel_config: ChannelConfig,
    file_chan: async_channel::Receiver<Result<File, Error>>,
    evs: Option<EventChunker>,
    buffer_size: u32,
}

impl EventBlobsComplete {

    pub fn new(query: &netpod::AggQuerySingleChannel, channel_config: ChannelConfig, node: Arc<Node>) -> Self {
        Self {
            file_chan: open_files(query, node),
            evs: None,
            buffer_size: query.buffer_size,
            channel_config,
        }
    }

}

impl Stream for EventBlobsComplete {
    type Item = Result<EventFull, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            let z = match &mut self.evs {
                Some(evs) => {
                    match evs.poll_next_unpin(cx) {
                        Ready(Some(k)) => {
                            Ready(Some(k))
                        }
                        Ready(None) => {
                            self.evs = None;
                            continue 'outer;
                        }
                        Pending => Pending,
                    }
                }
                None => {
                    match self.file_chan.poll_next_unpin(cx) {
                        Ready(Some(k)) => {
                            match k {
                                Ok(file) => {
                                    let inp = Box::pin(file_content_stream(file, self.buffer_size as usize));
                                    let mut chunker = EventChunker::new(inp, self.channel_config.clone());
                                    self.evs.replace(chunker);
                                    continue 'outer;
                                }
                                Err(e) => Ready(Some(Err(e)))
                            }
                        }
                        Ready(None) => Ready(None),
                        Pending => Pending,
                    }
                }
            };
            break z;
        }
    }

}


pub fn event_blobs_complete(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> impl Stream<Item=Result<EventFull, Error>> + Send {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let filerx = open_files(&query, node.clone());
        while let Ok(fileres) = filerx.recv().await {
            match fileres {
                Ok(file) => {
                    let inp = Box::pin(file_content_stream(file, query.buffer_size as usize));
                    let mut chunker = EventChunker::new(inp, todo!());
                    while let Some(evres) = chunker.next().await {
                        match evres {
                            Ok(evres) => {
                                yield Ok(evres);
                            }
                            Err(e) => {
                                yield Err(e)
                            }
                        }
                    }
                }
                Err(e) => {
                    yield Err(e);
                }
            }
        }
    }
}


pub struct EventChunker {
    inp: NeedMinBuffer,
    had_channel: bool,
    polled: u32,
    state: DataFileState,
    tmpbuf: Vec<u8>,
    channel_config: ChannelConfig,
}

enum DataFileState {
    FileHeader,
    Event,
}

impl EventChunker {

    pub fn new(inp: Pin<Box<dyn Stream<Item=Result<BytesMut, Error>> + Send>>, channel_config: ChannelConfig) -> Self {
        let mut inp = NeedMinBuffer::new(inp);
        inp.set_need_min(6);
        Self {
            inp: inp,
            had_channel: false,
            polled: 0,
            state: DataFileState::FileHeader,
            tmpbuf: vec![0; 1024 * 1024 * 4],
            channel_config,
        }
    }

    fn parse_buf(&mut self, buf: &mut BytesMut) -> Result<ParseResult, Error> {
        // must communicate to caller:
        // what I've found in the buffer
        // what I've consumed from the buffer
        // how many bytes I need min to make progress
        let mut ret = EventFull::empty();
        let mut need_min = 0 as u32;
        use byteorder::{BE, ReadBytesExt};
        //info!("parse_buf  rb {}", buf.len());
        //let mut i1 = 0;
        loop {
            //info!("parse_buf LOOP {}", i1);
            if (buf.len() as u32) < need_min {
                break;
            }
            match self.state {
                DataFileState::FileHeader => {
                    assert!(buf.len() >= 6, "logic");
                    let mut sl = std::io::Cursor::new(buf.as_ref());
                    let fver = sl.read_i16::<BE>().unwrap();
                    assert!(fver == 0, "unexpected file version");
                    let len = sl.read_i32::<BE>().unwrap();
                    assert!(len > 0 && len < 128, "unexpected data file header");
                    let totlen = len as usize + 2;
                    if buf.len() < totlen {
                        info!("parse_buf not enough A  totlen {}", totlen);
                        need_min = totlen as u32;
                        break;
                    }
                    else {
                        sl.advance(len as usize - 8);
                        let len2 = sl.read_i32::<BE>().unwrap();
                        assert!(len == len2, "len mismatch");
                        let s1 = String::from_utf8(buf.as_ref()[6..(len as usize + 6 - 8)].to_vec()).unwrap();
                        info!("channel name {}  len {}  len2 {}", s1, len, len2);
                        self.state = DataFileState::Event;
                        need_min = 4;
                        buf.advance(totlen);
                    }
                }
                DataFileState::Event => {
                    let mut sl = std::io::Cursor::new(buf.as_ref());
                    let len = sl.read_i32::<BE>().unwrap();
                    //info!("event len {}", len);
                    if (buf.len() as u32) < 20 {
                        // TODO gather stats about how often we find not enough input
                        //info!("parse_buf not enough B");
                        need_min = len as u32;
                        break;
                    }
                    else if (buf.len() as u32) < len as u32 {
                        // TODO this is just for testing
                        let mut sl = std::io::Cursor::new(buf.as_ref());
                        sl.read_i32::<BE>().unwrap();
                        sl.read_i64::<BE>().unwrap();
                        let ts = sl.read_i64::<BE>().unwrap();
                        //info!("parse_buf not enough C   len {}  have {}  ts {}", len, buf.len(), ts);
                        need_min = len as u32;
                        break;
                    }
                    else {
                        let mut sl = std::io::Cursor::new(buf.as_ref());
                        let len1b = sl.read_i32::<BE>().unwrap();
                        assert!(len == len1b);
                        sl.read_i64::<BE>().unwrap();
                        let ts = sl.read_i64::<BE>().unwrap() as u64;
                        let pulse = sl.read_i64::<BE>().unwrap() as u64;
                        sl.read_i64::<BE>().unwrap();
                        let _status = sl.read_i8().unwrap();
                        let _severity = sl.read_i8().unwrap();
                        let _optional = sl.read_i32::<BE>().unwrap();
                        assert!(_status == 0);
                        assert!(_severity == 0);
                        assert!(_optional == -1);
                        let type_flags = sl.read_u8().unwrap();
                        let type_index = sl.read_u8().unwrap();
                        assert!(type_index <= 13);
                        use dtflags::*;
                        let is_compressed = type_flags & COMPRESSION != 0;
                        let is_array = type_flags & ARRAY != 0;
                        let is_big_endian = type_flags & BIG_ENDIAN != 0;
                        let is_shaped = type_flags & SHAPE != 0;
                        if let Shape::Wave(_) = self.channel_config.shape {
                            assert!(is_array);
                        }
                        let compression_method = if is_compressed {
                            sl.read_u8().unwrap()
                        }
                        else {
                            0
                        };
                        let shape_dim = if is_shaped {
                            sl.read_u8().unwrap()
                        }
                        else {
                            0
                        };
                        assert!(compression_method <= 0);
                        assert!(!is_shaped || (shape_dim >= 1 && shape_dim <= 2));
                        let mut shape_lens = [0, 0, 0, 0];
                        for i1 in 0..shape_dim {
                            shape_lens[i1 as usize] = sl.read_u32::<BE>().unwrap();
                        }
                        if is_compressed {
                            //debug!("event  ts {}  is_compressed {}", ts, is_compressed);
                            let value_bytes = sl.read_u64::<BE>().unwrap();
                            let block_size = sl.read_u32::<BE>().unwrap();
                            let p1 = sl.position() as u32;
                            let k1 = len as u32 - p1 - 4;
                            //debug!("event  len {}  ts {}  is_compressed {}  shape_dim {}  len-dim-0 {}  value_bytes {}  block_size {}", len, ts, is_compressed, shape_dim, shape_lens[0], value_bytes, block_size);
                            assert!(value_bytes < 1024 * 256);
                            assert!(block_size < 1024 * 32);
                            //let value_bytes = value_bytes;
                            let type_size = type_size(type_index);
                            let ele_count = value_bytes / type_size as u64;
                            let ele_size = type_size;
                            match self.channel_config.shape {
                                Shape::Wave(ele2) => {
                                    assert!(ele2 == ele_count as u32);
                                }
                                _ => panic!(),
                            }
                            let decomp_bytes = (type_size * ele_count as u32) as usize;
                            let mut decomp = BytesMut::with_capacity(decomp_bytes);
                            unsafe {
                                decomp.set_len(decomp_bytes);
                            }
                            //debug!("try decompress  value_bytes {}  ele_size {}  ele_count {}  type_index {}", value_bytes, ele_size, ele_count, type_index);
                            let c1 = bitshuffle_decompress(&buf.as_ref()[p1 as usize..], &mut decomp, ele_count as usize, ele_size as usize, 0).unwrap();
                            //debug!("decompress result  c1 {}  k1 {}", c1, k1);
                            assert!(c1 as u32 == k1);
                            ret.add_event(ts, pulse, Some(decomp), ScalarType::from_dtype_index(type_index));
                        }
                        else {
                            todo!()
                        }
                        buf.advance(len as usize);
                        need_min = 4;
                    }
                }
            }
            //i1 += 1;
        }
        Ok(ParseResult {
            events: ret,
            need_min: need_min,
        })
    }

}

fn type_size(ix: u8) -> u32 {
    match ix {
        0 => 1,
        1 => 1,
        2 => 1,
        3 => 1,
        4 => 2,
        5 => 2,
        6 => 2,
        7 => 4,
        8 => 4,
        9 => 8,
        10 => 8,
        11 => 4,
        12 => 8,
        13 => 1,
        _ => panic!("logic")
    }
}

struct ParseResult {
    events: EventFull,
    need_min: u32,
}

impl Stream for EventChunker {
    type Item = Result<EventFull, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polled += 1;
        if self.polled >= 2000000 {
            warn!("EventChunker poll limit reached");
            return Poll::Ready(None);
        }
        let g = &mut self.inp;
        pin_mut!(g);
        match g.poll_next(cx) {
            Poll::Ready(Some(Ok(mut buf))) => {
                //info!("EventChunker got buffer  len {}", buf.len());
                match self.parse_buf(&mut buf) {
                    Ok(res) => {
                        if buf.len() > 0 {

                            // TODO gather stats about this:
                            //info!("parse_buf returned {} leftover bytes to me", buf.len());
                            self.inp.put_back(buf);

                        }
                        if res.need_min > 8000 {
                            warn!("spurious EventChunker asks for need_min {}", res.need_min);
                            panic!();
                        }
                        self.inp.set_need_min(res.need_min);
                        Poll::Ready(Some(Ok(res.events)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e.into())))
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

}

pub struct EventFull {
    tss: Vec<u64>,
    pulses: Vec<u64>,
    decomps: Vec<Option<BytesMut>>,
    scalar_types: Vec<ScalarType>,
}

impl EventFull {

    pub fn empty() -> Self {
        Self {
            tss: vec![],
            pulses: vec![],
            decomps: vec![],
            scalar_types: vec![],
        }
    }

    fn add_event(&mut self, ts: u64, pulse: u64, decomp: Option<BytesMut>, scalar_type: ScalarType) {
        self.tss.push(ts);
        self.pulses.push(pulse);
        self.decomps.push(decomp);
        self.scalar_types.push(scalar_type);
    }

}




pub struct NeedMinBuffer {
    inp: Pin<Box<dyn Stream<Item=Result<BytesMut, Error>> + Send>>,
    need_min: u32,
    left: Option<BytesMut>,
}

impl NeedMinBuffer {

    pub fn new(inp: Pin<Box<dyn Stream<Item=Result<BytesMut, Error>> + Send>>) -> Self {
        Self {
            inp: inp,
            need_min: 1,
            left: None,
        }
    }

    pub fn put_back(&mut self, buf: BytesMut) {
        assert!(self.left.is_none());
        self.left = Some(buf);
    }

    pub fn set_need_min(&mut self, need_min: u32) {
        self.need_min = need_min;
    }

}

impl Stream for NeedMinBuffer {
    type Item = Result<BytesMut, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            let mut again = false;
            let g = &mut self.inp;
            pin_mut!(g);
            let z = match g.poll_next(cx) {
                Poll::Ready(Some(Ok(buf))) => {
                    //info!("NeedMin got buf  len {}", buf.len());
                    match self.left.take() {
                        Some(mut left) => {
                            left.unsplit(buf);
                            let buf = left;
                            if buf.len() as u32 >= self.need_min {
                                //info!("with left ready  len {}  need_min {}", buf.len(), self.need_min);
                                Poll::Ready(Some(Ok(buf)))
                            }
                            else {
                                //info!("with left not enough  len {}  need_min {}", buf.len(), self.need_min);
                                self.left.replace(buf);
                                again = true;
                                Poll::Pending
                            }
                        }
                        None => {
                            if buf.len() as u32 >= self.need_min {
                                //info!("simply ready  len {}  need_min {}", buf.len(), self.need_min);
                                Poll::Ready(Some(Ok(buf)))
                            }
                            else {
                                //info!("no previous leftover, need more  len {}  need_min {}", buf.len(), self.need_min);
                                self.left.replace(buf);
                                again = true;
                                Poll::Pending
                            }
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
            if !again {
                break z;
            }
        }
    }

}



pub fn raw_concat_channel_read_stream(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> impl Stream<Item=Result<Bytes, Error>> + Send {
    let mut query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let mut i1 = 0;
        loop {
            let timebin = 18700 + i1;
            query.timebin = timebin;
            let s2 = raw_concat_channel_read_stream_timebin(&query, node.clone());
            pin_mut!(s2);
            while let Some(item) = s2.next().await {
                yield item;
            }
            i1 += 1;
            if i1 > 15 {
                break;
            }
        }
    }
}


pub fn raw_concat_channel_read_stream_timebin(query: &netpod::AggQuerySingleChannel, node: Arc<Node>) -> impl Stream<Item=Result<Bytes, Error>> {
    let query = query.clone();
    let node = node.clone();
    async_stream::stream! {
        let path = datapath(query.timebin as u64, &query.channel_config, &node);
        debug!("try path: {:?}", path);
        let mut fin = OpenOptions::new().read(true).open(path).await?;
        let meta = fin.metadata().await?;
        debug!("file meta {:?}", meta);
        let blen = query.buffer_size as usize;
        use tokio::io::AsyncReadExt;
        loop {
            let mut buf = BytesMut::with_capacity(blen);
            assert!(buf.is_empty());
            if false {
                buf.resize(buf.capacity(), 0);
                if buf.as_mut().len() != blen {
                    panic!("logic");
                }
            }
            let n1 = fin.read_buf(&mut buf).await?;
            if n1 == 0 {
                break;
            }
            yield Ok(buf.freeze());
        }
    }
}


fn datapath(timebin: u64, config: &netpod::ChannelConfig, node: &Node) -> PathBuf {
    //let pre = "/data/sf-databuffer/daq_swissfel";
    node.data_base_path
    .join(format!("{}_{}", node.ksprefix, config.keyspace))
    .join("byTime")
    .join(config.channel.name.clone())
    .join(format!("{:019}", timebin))
    .join(format!("{:010}", node.split))
    .join(format!("{:019}_00000_Data", config.time_bin_size / netpod::timeunits::MS))
}


/**
Read all events from all timebins for the given channel and split.
*/
#[allow(dead_code)]
pub struct RawConcatChannelReader {
    ksprefix: String,
    keyspace: u32,
    channel: netpod::Channel,
    split: u32,
    tbsize: u32,
    buffer_size: u32,
    tb: u32,
    //file_reader: Option<FileReader>,

    // TODO
    // Not enough to store a simple future here.
    // That will only resolve to a single output.
    // • How can I transition between Stream and async world?
    // • I guess I must not poll a completed Future which comes from some async fn again after it completed.
    // • relevant crates:  async-stream, tokio-stream
    fopen: Option<Box<dyn Future<Output=Option<Result<Bytes, Error>>> + Send>>,
}

impl RawConcatChannelReader {

    pub fn read(self) -> Result<netpod::BodyStream, Error> {
        let res = netpod::BodyStream {
            inner: Box::new(self),
        };
        Ok(res)
    }

}

impl futures_core::Stream for RawConcatChannelReader {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }

}

pub mod dtflags {
    pub const COMPRESSION: u8 = 0x80;
    pub const ARRAY: u8 = 0x40;
    pub const BIG_ENDIAN: u8 = 0x20;
    pub const SHAPE: u8 = 0x10;
}


trait ChannelConfigExt {
    fn dtflags(&self) -> u8;
}

impl ChannelConfigExt for ChannelConfig {

    fn dtflags(&self) -> u8 {
        let mut ret = 0;
        if self.compression { ret |= COMPRESSION; }
        match self.shape {
            Shape::Scalar => {}
            Shape::Wave(_) => { ret |= SHAPE; }
        }
        if self.big_endian { ret |= BIG_ENDIAN; }
        if self.array { ret |= ARRAY; }
        ret
    }

}
