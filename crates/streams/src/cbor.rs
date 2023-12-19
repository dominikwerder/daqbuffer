use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Events;
use items_0::WithLen;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use netpod::log::Level;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use std::fmt;
use std::io::Cursor;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

trait ErrConv<T> {
    fn ec(self) -> Result<T, Error>;
}

impl<T, K> ErrConv<T> for Result<T, ciborium::de::Error<K>>
where
    K: fmt::Debug,
{
    fn ec(self) -> Result<T, Error> {
        self.map_err(|e| Error::from_string(format!("{e}")))
    }
}

pub struct CborBytes(Bytes);

impl CborBytes {
    pub fn into_inner(self) -> Bytes {
        self.0
    }

    pub fn len(&self) -> u32 {
        self.0.len() as _
    }
}

impl WithLen for CborBytes {
    fn len(&self) -> usize {
        self.len() as usize
    }
}

impl From<CborBytes> for Bytes {
    fn from(value: CborBytes) -> Self {
        value.0
    }
}

pub type CborStream = Pin<Box<dyn Stream<Item = Result<CborBytes, Error>> + Send>>;

pub type SitemtyDynEventsStream =
    Pin<Box<dyn Stream<Item = Result<StreamItem<RangeCompletableItem<Box<dyn Events>>>, Error>> + Send>>;

pub fn events_stream_to_cbor_stream(stream: SitemtyDynEventsStream) -> impl Stream<Item = Result<CborBytes, Error>> {
    let stream = stream.map(|x| match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(evs) => {
                    if false {
                        use items_0::AsAnyRef;
                        // TODO impl generically on EventsDim0 ?
                        if let Some(evs) = evs.as_any_ref().downcast_ref::<items_2::eventsdim0::EventsDim0<f64>>() {
                            let mut buf = Vec::new();
                            ciborium::into_writer(evs, &mut buf)
                                .map_err(|e| Error::with_msg_no_trace(format!("{e}")))?;
                            let bytes = Bytes::from(buf);
                            let _item = CborBytes(bytes);
                            // Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                        } else {
                            let _item = LogItem::from_node(0, Level::DEBUG, format!("cbor stream discarded item"));
                            // Ok(StreamItem::Log(item))
                        };
                    }
                    let buf = evs.to_cbor_vec_u8();
                    let bytes = Bytes::from(buf);
                    let item = CborBytes(bytes);
                    Ok(item)
                }
                RangeCompletableItem::RangeComplete => {
                    use ciborium::cbor;
                    let item = cbor!({
                        "rangeFinal" => true,
                    })
                    .map_err(Error::from_string)?;
                    let mut buf = Vec::with_capacity(64);
                    ciborium::into_writer(&item, &mut buf).map_err(Error::from_string)?;
                    let bytes = Bytes::from(buf);
                    let item = CborBytes(bytes);
                    Ok(item)
                }
            },
            StreamItem::Log(item) => {
                info!("{item:?}");
                let item = CborBytes(Bytes::new());
                Ok(item)
            }
            StreamItem::Stats(item) => {
                info!("{item:?}");
                let item = CborBytes(Bytes::new());
                Ok(item)
            }
        },
        Err(e) => {
            use ciborium::cbor;
            let item = cbor!({
                "error" => e.to_string(),
            })
            .map_err(Error::from_string)?;
            let mut buf = Vec::with_capacity(64);
            ciborium::into_writer(&item, &mut buf).map_err(Error::from_string)?;
            let bytes = Bytes::from(buf);
            let item = CborBytes(bytes);
            Ok(item)
        }
    });
    stream
}

pub struct FramedBytesToSitemtyDynEventsStream<S> {
    inp: S,
    scalar_type: ScalarType,
    shape: Shape,
    buf: BytesMut,
}

impl<S> FramedBytesToSitemtyDynEventsStream<S> {
    pub fn new(inp: S, scalar_type: ScalarType, shape: Shape) -> Self {
        Self {
            inp,
            scalar_type,
            shape,
            buf: BytesMut::with_capacity(1024 * 64),
        }
    }

    fn try_parse(&mut self) -> Result<Option<Sitemty<Box<dyn Events>>>, Error> {
        // debug!("try_parse {}", self.buf.len());
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let n = u32::from_le_bytes(self.buf[..4].try_into()?);
        if n > 1024 * 1024 * 40 {
            let e = Error::with_msg_no_trace(format!("frame too large {n}"));
            error!("{e}");
            return Err(e);
        }
        if self.buf.len() < 4 + n as usize {
            // debug!("not enough  {}  {}", n, self.buf.len());
            return Ok(None);
        }
        let buf = &self.buf[4..4 + n as usize];
        let val: ciborium::Value = ciborium::from_reader(std::io::Cursor::new(buf)).map_err(Error::from_string)?;
        // debug!("decoded ciborium value {val:?}");
        let item = if let Some(map) = val.as_map() {
            if let Some(x) = map.get(0) {
                if let Some(y) = x.0.as_text() {
                    if y == "rangeFinal" {
                        if let Some(y) = x.1.as_bool() {
                            if y {
                                Some(StreamItem::DataItem(
                                    RangeCompletableItem::<Box<dyn Events>>::RangeComplete,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let item = if let Some(x) = item {
            Some(x)
        } else {
            let item = decode_cbor_to_box_events(buf, &self.scalar_type, &self.shape)?;
            Some(StreamItem::DataItem(RangeCompletableItem::Data(item)))
        };
        self.buf.advance(4 + n as usize);
        if let Some(x) = item {
            Ok(Some(Ok(x)))
        } else {
            let item = LogItem::from_node(0, Level::DEBUG, format!("decoded ciborium Value"));
            Ok(Some(Ok(StreamItem::Log(item))))
        }
    }
}

impl<S> Stream for FramedBytesToSitemtyDynEventsStream<S>
where
    S: Stream<Item = Result<Bytes, Error>> + Unpin,
{
    type Item = <SitemtyDynEventsStream as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.try_parse() {
                Ok(Some(x)) => Ready(Some(x)),
                Ok(None) => match self.inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => match x {
                        Ok(x) => {
                            self.buf.put_slice(&x);
                            continue;
                        }
                        Err(e) => Ready(Some(Err(e))),
                    },
                    Ready(None) => {
                        if self.buf.len() > 0 {
                            warn!("remaining bytes in input buffer, input closed  len {}", self.buf.len());
                        }
                        Ready(None)
                    }
                    Pending => Pending,
                },
                Err(e) => Ready(Some(Err(e))),
            };
        }
    }
}

macro_rules! cbor_scalar {
    ($ty:ident, $buf:expr) => {{
        type T = $ty;
        type C = EventsDim0<T>;
        let item: C = ciborium::from_reader(Cursor::new($buf)).ec()?;
        Box::new(item)
    }};
}

macro_rules! cbor_wave {
    ($ty:ident, $buf:expr) => {{
        type T = $ty;
        type C = EventsDim1<T>;
        let item: C = ciborium::from_reader(Cursor::new($buf)).ec()?;
        Box::new(item)
    }};
}

fn decode_cbor_to_box_events(buf: &[u8], scalar_type: &ScalarType, shape: &Shape) -> Result<Box<dyn Events>, Error> {
    let item: Box<dyn Events> = match shape {
        Shape::Scalar => match scalar_type {
            ScalarType::U8 => cbor_scalar!(u8, buf),
            ScalarType::U16 => cbor_scalar!(u16, buf),
            ScalarType::U32 => cbor_scalar!(u32, buf),
            ScalarType::U64 => cbor_scalar!(u64, buf),
            ScalarType::I8 => cbor_scalar!(i8, buf),
            ScalarType::I16 => cbor_scalar!(i16, buf),
            ScalarType::I32 => cbor_scalar!(i32, buf),
            ScalarType::I64 => cbor_scalar!(i64, buf),
            ScalarType::F32 => cbor_scalar!(f32, buf),
            ScalarType::F64 => cbor_scalar!(f64, buf),
            _ => {
                return Err(Error::from_string(format!(
                    "decode_cbor_to_box_events  {:?}  {:?}",
                    scalar_type, shape
                )))
            }
        },
        Shape::Wave(_) => match scalar_type {
            ScalarType::U8 => cbor_wave!(u8, buf),
            ScalarType::U16 => cbor_wave!(u16, buf),
            ScalarType::I64 => cbor_wave!(i64, buf),
            _ => {
                return Err(Error::from_string(format!(
                    "decode_cbor_to_box_events  {:?}  {:?}",
                    scalar_type, shape
                )))
            }
        },
        Shape::Image(_, _) => todo!(),
    };
    Ok(item)
}
