use crate::get_url_query_pairs;
use crate::log::*;
use crate::AppendToUrl;
use crate::BinnedRange;
use crate::FromUrl;
use crate::NanoRange;
use err::Error;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventTransform {
    EventBlobsVerbatim,
    EventBlobsUncompressed,
    ValueFull,
    ArrayPick(usize),
    MinMaxAvgDev,
    PulseIdDiff,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeBinningTransform {
    None,
    TimeWeighted(BinnedRange),
    Unweighted(BinnedRange),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transform {
    event: EventTransform,
    time_binning: TimeBinningTransform,
}

impl Transform {
    fn url_prefix() -> &'static str {
        "transform"
    }
}

impl FromUrl for Transform {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let upre = Self::url_prefix();
        let key = "binningScheme";
        if let Some(s) = pairs.get(key) {
            let ret = if s == "eventBlobs" {
                Transform {
                    event: EventTransform::EventBlobsVerbatim,
                    time_binning: TimeBinningTransform::None,
                }
            } else if s == "fullValue" {
                Transform {
                    event: EventTransform::ValueFull,
                    time_binning: TimeBinningTransform::None,
                }
            } else if s == "timeWeightedScalar" {
                Transform {
                    event: EventTransform::MinMaxAvgDev,
                    time_binning: TimeBinningTransform::TimeWeighted(BinnedRange::covering_range(
                        NanoRange {
                            beg: 20000000000,
                            end: 30000000000,
                        },
                        20,
                    )?),
                }
            } else if s == "unweightedScalar" {
                Transform {
                    event: EventTransform::EventBlobsVerbatim,
                    time_binning: TimeBinningTransform::None,
                }
            } else if s == "binnedX" {
                let _u: usize = pairs.get("binnedXcount").map_or("1", |k| k).parse()?;
                warn!("TODO binnedXcount");
                Transform {
                    event: EventTransform::MinMaxAvgDev,
                    time_binning: TimeBinningTransform::None,
                }
            } else if s == "pulseIdDiff" {
                Transform {
                    event: EventTransform::PulseIdDiff,
                    time_binning: TimeBinningTransform::None,
                }
            } else {
                return Err(Error::with_msg("can not extract binningScheme"));
            };
            Ok(ret)
        } else {
            // TODO add option to pick from array.
            let _pick = pairs
                .get(&format!("{}ArrayPick", upre))
                .map(|x| match x.parse::<usize>() {
                    Ok(n) => Some(n),
                    Err(_) => None,
                })
                .unwrap_or(None);
            let ret = Transform {
                event: EventTransform::EventBlobsVerbatim,
                time_binning: TimeBinningTransform::None,
            };
            Ok(ret)
        }
    }
}

impl AppendToUrl for Transform {
    fn append_to_url(&self, url: &mut Url) {
        warn!("TODO AppendToUrl for Transform");
        let upre = Self::url_prefix();
        let mut g = url.query_pairs_mut();
        if let Some(x) = &Some(123) {
            g.append_pair(&format!("{}ArrayPick", upre), &format!("{}", x));
        }
    }
}
