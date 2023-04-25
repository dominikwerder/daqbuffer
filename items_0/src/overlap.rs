use netpod::log::*;
use netpod::range::evrange::SeriesRange;

// TODO rename, no more deque involved
pub trait HasTimestampDeque {
    fn timestamp_min(&self) -> Option<u64>;
    fn timestamp_max(&self) -> Option<u64>;
    fn pulse_min(&self) -> Option<u64>;
    fn pulse_max(&self) -> Option<u64>;
}

pub trait RangeOverlapCmp {
    fn range_overlap_cmp_beg(a: u64, b: u64) -> bool;
    fn range_overlap_cmp_end(a: u64, b: u64) -> bool;
}

pub trait RangeOverlapInfo {
    fn ends_before(&self, range: &SeriesRange) -> bool;
    fn ends_after(&self, range: &SeriesRange) -> bool;
    fn starts_after(&self, range: &SeriesRange) -> bool;
}

#[macro_export]
macro_rules! impl_range_overlap_info_events {
    ($ty:ident) => {
        impl<STY> RangeOverlapInfo for $ty<STY>
        where
            STY: ScalarOps,
        {
            fn ends_before(&self, range: &SeriesRange) -> bool {
                if range.is_time() {
                    if let Some(max) = HasTimestampDeque::timestamp_max(self) {
                        max < range.beg_u64()
                        //<Self as RangeOverlapCmp>::range_overlap_cmp_beg(max, range.beg_u64())
                    } else {
                        true
                    }
                } else if range.is_pulse() {
                    if let Some(max) = HasTimestampDeque::pulse_max(self) {
                        max < range.beg_u64()
                    } else {
                        true
                    }
                } else {
                    error!("unexpected");
                    true
                }
            }

            fn ends_after(&self, range: &SeriesRange) -> bool {
                if range.is_time() {
                    if let Some(max) = HasTimestampDeque::timestamp_max(self) {
                        max >= range.beg_u64()
                    } else {
                        true
                    }
                } else if range.is_pulse() {
                    if let Some(max) = HasTimestampDeque::pulse_max(self) {
                        max >= range.beg_u64()
                    } else {
                        true
                    }
                } else {
                    error!("unexpected");
                    false
                }
            }

            fn starts_after(&self, range: &SeriesRange) -> bool {
                if range.is_time() {
                    if let Some(min) = HasTimestampDeque::timestamp_min(self) {
                        min >= range.end_u64()
                    } else {
                        true
                    }
                } else if range.is_pulse() {
                    if let Some(min) = HasTimestampDeque::pulse_min(self) {
                        min >= range.end_u64()
                    } else {
                        true
                    }
                } else {
                    error!("unexpected");
                    true
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_range_overlap_info_bins {
    ($ty:ident) => {
        impl<STY> RangeOverlapInfo for $ty<STY>
        where
            STY: ScalarOps,
        {
            fn ends_before(&self, range: &SeriesRange) -> bool {
                if range.is_time() {
                    if let Some(max) = HasTimestampDeque::timestamp_max(self) {
                        max <= range.beg_u64()
                    } else {
                        true
                    }
                } else if range.is_pulse() {
                    // TODO for the time being, the ts represent either ts or pulse
                    if let Some(max) = HasTimestampDeque::timestamp_max(self) {
                        max <= range.beg_u64()
                    } else {
                        true
                    }
                } else {
                    error!("unexpected");
                    true
                }
            }

            fn ends_after(&self, range: &SeriesRange) -> bool {
                if range.is_time() {
                    if let Some(max) = HasTimestampDeque::timestamp_max(self) {
                        max > range.end_u64()
                    } else {
                        true
                    }
                } else if range.is_pulse() {
                    if let Some(max) = HasTimestampDeque::timestamp_max(self) {
                        max > range.end_u64()
                    } else {
                        true
                    }
                } else {
                    error!("unexpected");
                    false
                }
            }

            fn starts_after(&self, range: &SeriesRange) -> bool {
                if range.is_time() {
                    if let Some(min) = HasTimestampDeque::timestamp_min(self) {
                        min >= range.end_u64()
                    } else {
                        true
                    }
                } else if range.is_pulse() {
                    if let Some(min) = HasTimestampDeque::timestamp_min(self) {
                        min >= range.end_u64()
                    } else {
                        true
                    }
                } else {
                    error!("unexpected");
                    true
                }
            }
        }
    };
}
