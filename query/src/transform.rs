use err::Error;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::AppendToUrl;
use netpod::FromUrl;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum EventTransformQuery {
    EventBlobsVerbatim,
    EventBlobsUncompressed,
    ValueFull,
    ArrayPick(usize),
    // TODO should rename to scalar? dim0 will only stay a scalar.
    MinMaxAvgDev,
    PulseIdDiff,
}

impl EventTransformQuery {
    pub fn need_value_data(&self) -> bool {
        match self {
            EventTransformQuery::EventBlobsVerbatim => true,
            EventTransformQuery::EventBlobsUncompressed => true,
            EventTransformQuery::ValueFull => true,
            EventTransformQuery::ArrayPick(_) => true,
            EventTransformQuery::MinMaxAvgDev => true,
            EventTransformQuery::PulseIdDiff => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TimeBinningTransformQuery {
    None,
    TimeWeighted,
    Unweighted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformQuery {
    event: EventTransformQuery,
    time_binning: TimeBinningTransformQuery,
}

impl TransformQuery {
    fn url_prefix() -> &'static str {
        "transform"
    }

    pub fn default_events() -> Self {
        Self {
            event: EventTransformQuery::ValueFull,
            time_binning: TimeBinningTransformQuery::None,
        }
    }

    pub fn default_time_binned() -> Self {
        Self {
            event: EventTransformQuery::MinMaxAvgDev,
            time_binning: TimeBinningTransformQuery::TimeWeighted,
        }
    }

    pub fn is_default_events(&self) -> bool {
        self == &Self::default_events()
    }

    pub fn is_default_time_binned(&self) -> bool {
        self == &Self::default_time_binned()
    }

    pub fn for_event_blobs() -> Self {
        Self {
            event: EventTransformQuery::EventBlobsVerbatim,
            time_binning: TimeBinningTransformQuery::None,
        }
    }

    pub fn for_time_weighted_scalar() -> Self {
        Self {
            event: EventTransformQuery::MinMaxAvgDev,
            time_binning: TimeBinningTransformQuery::TimeWeighted,
        }
    }

    pub fn is_event_blobs(&self) -> bool {
        match &self.event {
            EventTransformQuery::EventBlobsVerbatim => true,
            EventTransformQuery::EventBlobsUncompressed => {
                error!("TODO decide on uncompressed event blobs");
                panic!()
            }
            _ => false,
        }
    }

    pub fn need_value_data(&self) -> bool {
        self.event.need_value_data()
    }

    pub fn is_pulse_id_diff(&self) -> bool {
        match &self.event {
            EventTransformQuery::PulseIdDiff => true,
            _ => false,
        }
    }

    pub fn build_event_transform(&self) -> () {}
}

impl FromUrl for TransformQuery {
    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let upre = Self::url_prefix();
        let key = "binningScheme";
        if let Some(s) = pairs.get(key) {
            let ret = if s == "eventBlobs" {
                TransformQuery {
                    event: EventTransformQuery::EventBlobsVerbatim,
                    time_binning: TimeBinningTransformQuery::None,
                }
            } else if s == "fullValue" {
                TransformQuery {
                    event: EventTransformQuery::ValueFull,
                    time_binning: TimeBinningTransformQuery::None,
                }
            } else if s == "timeWeightedScalar" {
                TransformQuery {
                    event: EventTransformQuery::MinMaxAvgDev,
                    time_binning: TimeBinningTransformQuery::TimeWeighted,
                }
            } else if s == "unweightedScalar" {
                TransformQuery {
                    event: EventTransformQuery::ValueFull,
                    time_binning: TimeBinningTransformQuery::None,
                }
            } else if s == "binnedX" {
                let _u: usize = pairs.get("binnedXcount").map_or("1", |k| k).parse()?;
                warn!("TODO binnedXcount");
                TransformQuery {
                    event: EventTransformQuery::MinMaxAvgDev,
                    time_binning: TimeBinningTransformQuery::None,
                }
            } else if s == "pulseIdDiff" {
                TransformQuery {
                    event: EventTransformQuery::PulseIdDiff,
                    time_binning: TimeBinningTransformQuery::None,
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
            let ret = TransformQuery {
                event: EventTransformQuery::ValueFull,
                time_binning: TimeBinningTransformQuery::None,
            };
            Ok(ret)
        }
    }
}

impl AppendToUrl for TransformQuery {
    fn append_to_url(&self, url: &mut Url) {
        warn!("TODO AppendToUrl for Transform");
        let upre = Self::url_prefix();
        let mut g = url.query_pairs_mut();
        if let Some(x) = &Some(123) {
            g.append_pair(&format!("{}ArrayPick", upre), &format!("{}", x));
        }
    }
}
