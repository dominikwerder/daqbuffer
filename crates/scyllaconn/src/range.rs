use netpod::range::evrange::SeriesRange;

#[derive(Debug, Clone)]
pub struct ScyllaSeriesRange {
    beg: u64,
    end: u64,
}

impl ScyllaSeriesRange {
    pub fn beg(&self) -> u64 {
        self.beg
    }

    pub fn end(&self) -> u64 {
        self.end
    }
}

impl From<&SeriesRange> for ScyllaSeriesRange {
    fn from(value: &SeriesRange) -> Self {
        match value {
            SeriesRange::TimeRange(k) => Self { beg: k.beg, end: k.end },
            SeriesRange::PulseRange(k) => Self { beg: k.beg, end: k.end },
        }
    }
}
