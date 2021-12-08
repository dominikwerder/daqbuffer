use netpod::log::*;
use std::time::Instant;

pub struct Timed {
    name: String,
    ts1: Instant,
}

impl Timed {
    pub fn new<T>(name: T) -> Self
    where
        T: ToString,
    {
        Self {
            name: name.to_string(),
            ts1: Instant::now(),
        }
    }
}

impl Drop for Timed {
    fn drop(&mut self) {
        let ts2 = Instant::now();
        let dt = ts2.duration_since(self.ts1);
        debug!("Timed {} {:?}", self.name, dt);
    }
}
