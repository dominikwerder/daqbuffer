use crate::agg::{Dim1F32Stream, ValuesDim1};
use crate::EventFull;
use err::Error;
use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

pub struct MergeDim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    inps: Vec<Dim1F32Stream<S>>,
    current: Vec<CurVal>,
    ixs: Vec<usize>,
    emitted_complete: bool,
    batch: ValuesDim1,
}

impl<S> MergeDim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>>,
{
    pub fn new(inps: Vec<Dim1F32Stream<S>>) -> Self {
        let n = inps.len();
        let mut current = vec![];
        for _ in 0..n {
            current.push(CurVal::None);
        }
        Self {
            inps,
            current: current,
            ixs: vec![0; n],
            emitted_complete: false,
            batch: ValuesDim1::empty(),
        }
    }
}

impl<S> Stream for MergeDim1F32Stream<S>
where
    S: Stream<Item = Result<EventFull, Error>> + Unpin,
{
    //type Item = <Dim1F32Stream as Stream>::Item;
    type Item = Result<ValuesDim1, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            if self.emitted_complete {
                panic!("poll on complete stream");
            }
            // can only run logic if all streams are either finished, errored or have some current value.
            for i1 in 0..self.inps.len() {
                match self.current[i1] {
                    CurVal::None => {
                        match self.inps[i1].poll_next_unpin(cx) {
                            Ready(Some(Ok(k))) => {
                                self.current[i1] = CurVal::Val(k);
                            }
                            Ready(Some(Err(e))) => {
                                // TODO emit this error, consider this stream as done, anything more to do here?
                                //self.current[i1] = CurVal::Err(e);
                                return Ready(Some(Err(e)));
                            }
                            Ready(None) => {
                                self.current[i1] = CurVal::Finish;
                            }
                            Pending => {
                                // TODO is this behaviour correct?
                                return Pending;
                            }
                        }
                    }
                    _ => (),
                }
            }
            let mut lowest_ix = usize::MAX;
            let mut lowest_ts = u64::MAX;
            for i1 in 0..self.inps.len() {
                match &self.current[i1] {
                    CurVal::Finish => {}
                    CurVal::Val(val) => {
                        let u = self.ixs[i1];
                        if u >= val.tss.len() {
                            self.ixs[i1] = 0;
                            self.current[i1] = CurVal::None;
                            continue 'outer;
                        } else {
                            let ts = val.tss[u];
                            if ts < lowest_ts {
                                lowest_ix = i1;
                                lowest_ts = ts;
                            }
                        }
                    }
                    _ => panic!(),
                }
            }
            if lowest_ix == usize::MAX {
                // TODO all inputs in finished state
                break Ready(None);
            } else {
                //trace!("decided on next lowest ts  {}  ix {}", lowest_ts, lowest_ix);
                self.batch.tss.push(lowest_ts);
                let rix = self.ixs[lowest_ix];
                match &mut self.current[lowest_ix] {
                    CurVal::Val(ref mut k) => {
                        let k = std::mem::replace(&mut k.values[rix], vec![]);
                        self.batch.values.push(k);
                    }
                    _ => panic!(),
                }
                self.ixs[lowest_ix] += 1;
            }
            if self.batch.tss.len() >= 64 {
                let k = std::mem::replace(&mut self.batch, ValuesDim1::empty());
                break Ready(Some(Ok(k)));
            }
        }
    }
}

enum CurVal {
    None,
    Finish,
    #[allow(dead_code)]
    Err(Error),
    Val(ValuesDim1),
}
