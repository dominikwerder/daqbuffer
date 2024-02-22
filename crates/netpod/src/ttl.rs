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
}
