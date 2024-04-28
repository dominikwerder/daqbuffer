use std::time::Duration;

#[derive(Debug, Clone)]
pub enum RetentionTime {
    Short,
    Medium,
    Long,
}

impl RetentionTime {
    pub fn table_prefix(&self) -> &'static str {
        use RetentionTime::*;
        match self {
            Short => "",
            Medium => "mt_",
            Long => "lt_",
        }
    }

    pub fn ttl_events_d0(&self) -> Duration {
        match self {
            RetentionTime::Short => Duration::from_secs(60 * 60 * 12),
            RetentionTime::Medium => Duration::from_secs(60 * 60 * 24 * 100),
            RetentionTime::Long => Duration::from_secs(60 * 60 * 24 * 31 * 12 * 11),
        }
    }

    pub fn ttl_events_d1(&self) -> Duration {
        match self {
            RetentionTime::Short => Duration::from_secs(60 * 60 * 12),
            RetentionTime::Medium => Duration::from_secs(60 * 60 * 24 * 100),
            RetentionTime::Long => Duration::from_secs(60 * 60 * 24 * 31 * 12 * 11),
        }
    }

    pub fn ttl_ts_msp(&self) -> Duration {
        let dt = self.ttl_events_d0();
        dt + dt / 30
    }

    pub fn ttl_binned(&self) -> Duration {
        self.ttl_events_d0() * 2
    }

    pub fn ttl_channel_status(&self) -> Duration {
        self.ttl_binned()
    }
}
