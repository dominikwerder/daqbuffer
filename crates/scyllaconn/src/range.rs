use netpod::range::evrange::SeriesRange;
use netpod::TsNano;

#[derive(Debug, Clone)]
pub struct ScyllaSeriesRange {
    beg: u64,
    end: u64,
}

impl ScyllaSeriesRange {
    pub fn beg(&self) -> TsNano {
        TsNano::from_ns(self.beg)
    }

    pub fn end(&self) -> TsNano {
        TsNano::from_ns(self.end)
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
