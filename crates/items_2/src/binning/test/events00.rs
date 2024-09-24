use crate::binning::container_events::ContainerEvents;
use crate::binning::timeweight::timeweight_events::BinnedEventsTimeweight;
use err::thiserror;
use err::ThisError;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::EnumVariant;
use netpod::TsNano;

#[derive(Debug, ThisError)]
#[cstm(name = "Error")]
enum Error {
    Timeweight(#[from] crate::binning::timeweight::timeweight_events::Error),
}

#[test]
fn test_bin_events_f32_simple_00() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    evs.push_back(TsNano::from_ms(103), 2.0);
    binner.ingest(evs)?;
    Ok(())
}

#[test]
fn test_bin_events_f32_simple_01() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    evs.push_back(TsNano::from_ms(103), 2.0);
    evs.push_back(TsNano::from_ms(104), 2.4);
    binner.ingest(evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    evs.push_back(TsNano::from_ms(111), 1.0);
    evs.push_back(TsNano::from_ms(112), 1.2);
    evs.push_back(TsNano::from_ms(113), 1.4);
    binner.ingest(evs)?;
    Ok(())
}

#[test]
fn test_bin_events_enum_simple_range_final() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::new();
    evs.push_back(TsNano::from_ms(103), EnumVariant::new(1, "one"));
    evs.push_back(TsNano::from_ms(104), EnumVariant::new(2, "two"));
    binner.ingest(evs)?;
    binner.range_final()?;
    Ok(())
}
