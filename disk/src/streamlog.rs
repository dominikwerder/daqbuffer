use items::LogItem;
use netpod::log::*;
use std::collections::VecDeque;

pub struct Streamlog {
    items: VecDeque<LogItem>,
    node_ix: u32,
}

impl Streamlog {
    pub fn new(node_ix: u32) -> Self {
        Self {
            items: VecDeque::new(),
            node_ix,
        }
    }

    pub fn append(&mut self, level: Level, msg: String) {
        let item = LogItem {
            node_ix: self.node_ix,
            level,
            msg,
        };
        self.items.push_back(item);
    }

    pub fn pop(&mut self) -> Option<LogItem> {
        self.items.pop_back()
    }

    pub fn emit(item: &LogItem) {
        match item.level {
            Level::ERROR => {
                error!("StreamLog  Node {}  {}", item.node_ix, item.msg);
            }
            Level::WARN => {
                warn!("StreamLog  Node {}  {}", item.node_ix, item.msg);
            }
            Level::INFO => {
                info!("StreamLog  Node {}  {}", item.node_ix, item.msg);
            }
            Level::DEBUG => {
                debug!("StreamLog  Node {}  {}", item.node_ix, item.msg);
            }
            Level::TRACE => {
                trace!("StreamLog  Node {}  {}", item.node_ix, item.msg);
            }
        }
    }
}
