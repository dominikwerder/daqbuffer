use netpod::log::*;
use serde::de::{Error, Visitor};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt::Formatter;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogItem {
    #[serde(with = "levelserde")]
    level: Level,
    msg: String,
}

struct VisitLevel;

impl<'de> Visitor<'de> for VisitLevel {
    type Value = u32;

    fn expecting(&self, fmt: &mut Formatter) -> std::fmt::Result {
        write!(fmt, "")
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(v)
    }
}

mod levelserde {
    use super::Level;
    use crate::streamlog::VisitLevel;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(t: &Level, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let g = match *t {
            Level::ERROR => 1,
            Level::WARN => 2,
            Level::INFO => 3,
            Level::DEBUG => 4,
            Level::TRACE => 5,
        };
        s.serialize_u32(g)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        match d.deserialize_u32(VisitLevel) {
            Ok(level) => {
                let g = if level == 1 {
                    Level::ERROR
                } else if level == 2 {
                    Level::WARN
                } else if level == 3 {
                    Level::INFO
                } else if level == 4 {
                    Level::DEBUG
                } else if level == 5 {
                    Level::TRACE
                } else {
                    Level::TRACE
                };
                Ok(g)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Streamlog {
    items: VecDeque<LogItem>,
}

impl Streamlog {
    pub fn new() -> Self {
        Self { items: VecDeque::new() }
    }

    pub fn append(&mut self, level: Level, msg: String) {
        let item = LogItem { level, msg };
        self.items.push_back(item);
    }

    pub fn pop(&mut self) -> Option<LogItem> {
        self.items.pop_back()
    }
}
