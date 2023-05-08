use async_channel::Send;
use async_channel::Sender;
use err::Error;
use futures_util::pin_mut;
use futures_util::Future;
use futures_util::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[pin_project::pin_project]
pub struct Itemclone<T, INP>
where
    T: 'static,
{
    #[pin]
    sender: Sender<T>,
    inp: INP,
    send_fut: Option<Send<'static, T>>,
}

impl<T, INP> Itemclone<T, INP> {
    pub fn new(inp: INP, sender: Sender<T>) -> Self
    where
        INP: Stream<Item = T> + Unpin,
        T: Clone + Unpin,
    {
        Self {
            sender,
            inp,
            send_fut: None,
        }
    }
}

impl<T, INP> Itemclone<T, INP>
where
    INP: Stream<Item = T> + Unpin,
    T: Clone + Unpin,
{
    fn poll_fresh(&mut self, cx: &mut Context) -> Poll<Option<Result<T, Error>>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                let sender = unsafe { &mut *((&mut self.sender) as *mut Sender<T>) };
                self.send_fut = Some(sender.send(item.clone()));
                Ready(Some(Ok(item)))
            }
            Ready(None) => {
                self.sender.close();
                Ready(None)
            }
            Pending => Pending,
        }
    }

    fn send_copy(fut: &mut Send<T>, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        pin_mut!(fut);
        match fut.poll(cx) {
            Ready(Ok(())) => Ready(Ok(())),
            Ready(Err(e)) => Ready(Err(e.into())),
            Pending => Pending,
        }
    }
}

impl<T, INP> Stream for Itemclone<T, INP>
where
    INP: Stream<Item = T> + Unpin,
    T: Clone + Unpin,
{
    type Item = Result<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let proj = self.as_mut().project();
        match self.send_fut.as_mut() {
            Some(fut) => match Self::send_copy(fut, cx) {
                Ready(Ok(())) => self.poll_fresh(cx),
                Ready(Err(e)) => Ready(Some(Err(e))),
                Pending => Pending,
            },
            None => self.poll_fresh(cx),
        }
    }
}
