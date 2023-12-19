use crate::cbor::CborBytes;
use futures_util::future;
use futures_util::Stream;
use futures_util::StreamExt;

pub fn non_empty<S, E>(inp: S) -> impl Stream<Item = Result<CborBytes, E>>
where
    S: Stream<Item = Result<CborBytes, E>>,
{
    inp.filter(|x| {
        future::ready(match x {
            Ok(x) => x.len() > 0,
            Err(_) => true,
        })
    })
}

pub fn only_first_err<S, T, E>(inp: S) -> impl Stream<Item = Result<T, E>>
where
    S: Stream<Item = Result<T, E>>,
{
    inp.take_while({
        let mut state = true;
        move |x| {
            let ret = state;
            if x.is_err() {
                state = false;
            }
            future::ready(ret)
        }
    })
}
