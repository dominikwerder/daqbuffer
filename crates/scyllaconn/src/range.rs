use core::fmt;
use netpod::range::evrange::SeriesRange;
use netpod::TsNano;

#[derive(Debug, Clone)]
pub struct ScyllaSeriesRange {
    beg: u64,
    end: u64,
}

impl ScyllaSeriesRange {
    pub fn new(beg: TsNano, end: TsNano) -> Self {
        Self {
            beg: beg.ns(),
            end: end.ns(),
        }
    }

    pub fn beg(&self) -> TsNano {
        TsNano::from_ns(self.beg)
    }

    pub fn end(&self) -> TsNano {
        TsNano::from_ns(self.end)
    }

    pub fn fmt(&self) -> ScyllaSeriesRangeFmt {
        ScyllaSeriesRangeFmt { val: self.clone() }
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

pub struct ScyllaSeriesRangeFmt {
    val: ScyllaSeriesRange,
}

impl fmt::Display for ScyllaSeriesRangeFmt {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "ScyllaSeriesRange {{ beg: {}, end: {} }}",
            self.val.beg().fmt(),
            self.val.end().fmt()
        )
    }
}
