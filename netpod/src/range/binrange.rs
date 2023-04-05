use super::evrange::NanoRange;
use super::evrange::SeriesRange;
use crate::timeunits::SEC;
use crate::BinnedRangeEnum;
use crate::Dim0Kind;
use crate::TsNano;
use chrono::DateTime;
use chrono::Utc;

#[test]
fn test_binned_range_covering_00() {
    let range = SeriesRange::TimeRange(NanoRange::from_date_time(
        DateTime::parse_from_rfc3339("1970-01-01T10:10:00Z").unwrap().into(),
        DateTime::parse_from_rfc3339("1970-01-01T10:20:00Z").unwrap().into(),
    ));
    let r = BinnedRangeEnum::covering_range(range, 9).unwrap();
    assert_eq!(r.bin_count(), 10);
    if let Dim0Kind::Time = r.dim0kind() {
    } else {
        panic!()
    }
    let r2 = r.binned_range_time();
    let a = r2.edges();
    assert_eq!(a.len(), 11);
    assert_eq!(a[0], TsNano((((10 * 60) + 10) * 60 + 0) * SEC));
    assert_eq!(a[1], TsNano((((10 * 60) + 11) * 60 + 0) * SEC));
    assert_eq!(a[10], TsNano((((10 * 60) + 20) * 60 + 0) * SEC));
}
