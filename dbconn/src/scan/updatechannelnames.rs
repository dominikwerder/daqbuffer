use super::get_node_disk_ident;
use super::update_db_with_channel_name_list;
use super::FindChannelNamesFromConfigReadDir;
use super::NodeDiskIdent;
use super::UpdatedDbWithChannelNames;
use crate::create_connection;
use crate::pg::Client as PgClient;
use err::Error;
use futures_util::Future;
use futures_util::Stream;
use netpod::NodeConfigCached;
use pin_project::pin_project;
use std::os::unix::prelude::OsStringExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[pin_project]
struct UpdatedDbWithChannelNamesStream {
    errored: bool,
    data_complete: bool,
    #[allow(dead_code)]
    node_config: Pin<Box<NodeConfigCached>>,
    // TODO can we pass a Pin to the async fn instead of creating static ref?
    node_config_ref: &'static NodeConfigCached,
    #[pin]
    client_fut: Option<Pin<Box<dyn Future<Output = Result<PgClient, Error>> + Send>>>,
    #[pin]
    client: Option<PgClient>,
    client_ref: Option<&'static PgClient>,
    #[pin]
    ident_fut: Option<Pin<Box<dyn Future<Output = Result<NodeDiskIdent, Error>> + Send>>>,
    ident: Option<NodeDiskIdent>,
    #[pin]
    find: Option<FindChannelNamesFromConfigReadDir>,
    #[pin]
    update_batch: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
    channel_inp_done: bool,
    clist: Vec<String>,
}

impl UpdatedDbWithChannelNamesStream {
    #[allow(unused)]
    fn new(node_config: NodeConfigCached) -> Result<Self, Error> {
        let node_config = Box::pin(node_config.clone());
        let node_config_ref = unsafe { &*(&node_config as &NodeConfigCached as *const _) };
        let mut ret = Self {
            errored: false,
            data_complete: false,
            node_config,
            node_config_ref,
            client_fut: None,
            client: None,
            client_ref: None,
            ident_fut: None,
            ident: None,
            find: None,
            update_batch: None,
            channel_inp_done: false,
            clist: Vec::new(),
        };
        ret.client_fut = Some(Box::pin(create_connection(
            &ret.node_config_ref.node_config.cluster.database,
        )));
        Ok(ret)
    }
}

impl Stream for UpdatedDbWithChannelNamesStream {
    type Item = Result<UpdatedDbWithChannelNames, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut pself = self.project();
        loop {
            break if *pself.errored {
                Ready(None)
            } else if *pself.data_complete {
                Ready(None)
            } else if let Some(fut) = pself.find.as_mut().as_pin_mut() {
                match fut.poll_next(cx) {
                    Ready(Some(Ok(item))) => {
                        pself
                            .clist
                            .push(String::from_utf8(item.file_name().into_vec()).unwrap());
                        continue;
                    }
                    Ready(Some(Err(e))) => {
                        *pself.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        *pself.channel_inp_done = true;
                        // Work through the collected items
                        let l = std::mem::replace(pself.clist, Vec::new());
                        let fut = update_db_with_channel_name_list(
                            l,
                            pself.ident.as_ref().unwrap().facility,
                            pself.client.as_ref().get_ref().as_ref().unwrap(),
                        );
                        // TODO
                        //pself.update_batch.replace(Box::pin(fut));
                        let _ = fut;
                        continue;
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = pself.ident_fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Ready(Ok(item)) => {
                        *pself.ident_fut = None;
                        *pself.ident = Some(item);
                        let ret = UpdatedDbWithChannelNames {
                            msg: format!("Got ident {:?}", pself.ident),
                            count: 43,
                        };
                        let base_path = &pself
                            .node_config
                            .node
                            .sf_databuffer
                            .as_ref()
                            .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
                            .data_base_path;
                        let s = FindChannelNamesFromConfigReadDir::new(base_path);
                        *pself.find = Some(s);
                        Ready(Some(Ok(ret)))
                    }
                    Ready(Err(e)) => {
                        *pself.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = pself.client_fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Ready(Ok(item)) => {
                        *pself.client_fut = None;
                        //*pself.client = Some(Box::pin(item));
                        //*pself.client_ref = Some(unsafe { &*(&pself.client.as_ref().unwrap() as &Client as *const _) });
                        *pself.client = Some(item);
                        let c2: &PgClient = pself.client.as_ref().get_ref().as_ref().unwrap();
                        *pself.client_ref = Some(unsafe { &*(c2 as *const _) });

                        //() == pself.node_config.as_ref();
                        //() == pself.client.as_ref().as_pin_ref().unwrap();
                        /* *pself.ident_fut = Some(Box::pin(get_node_disk_ident_2(
                            pself.node_config.as_ref(),
                            pself.client.as_ref().as_pin_ref().unwrap(),
                        )));*/
                        *pself.ident_fut = Some(Box::pin(get_node_disk_ident(
                            pself.node_config_ref,
                            pself.client_ref.as_ref().unwrap(),
                        )));
                        let ret = UpdatedDbWithChannelNames {
                            msg: format!("Client opened connection"),
                            count: 42,
                        };
                        Ready(Some(Ok(ret)))
                    }
                    Ready(Err(e)) => {
                        *pself.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                }
            } else {
                Ready(None)
            };
        }
    }
}
