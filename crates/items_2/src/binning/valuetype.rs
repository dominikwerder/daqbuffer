use super::aggregator::AggregatorTimeWeight;
use super::binnedvaluetype::BinnedNumericValue;
use super::container_events::Container;
use super::container_events::EventValueType;
use crate::vecpreview::PreviewRange;
use core::fmt;
use netpod::DtNano;
use netpod::EnumVariant;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumVariantContainer {
    ixs: VecDeque<u16>,
    names: VecDeque<String>,
}

impl PreviewRange for EnumVariantContainer {
    fn preview<'a>(&'a self) -> Box<dyn fmt::Debug + 'a> {
        let ret = crate::vecpreview::PreviewCell {
            a: self.ixs.front(),
            b: self.ixs.back(),
        };
        Box::new(ret)
    }
}

impl Container<EnumVariant> for EnumVariantContainer {
    fn new() -> Self {
        Self {
            ixs: VecDeque::new(),
            names: VecDeque::new(),
        }
    }

    fn push_back(&mut self, val: EnumVariant) {
        let (ix, name) = val.into_parts();
        self.ixs.push_back(ix);
        self.names.push_back(name);
    }

    fn pop_front(&mut self) -> Option<EnumVariant> {
        if let (Some(a), Some(b)) = (self.ixs.pop_front(), self.names.pop_front()) {
            Some(EnumVariant::new(a, b))
        } else {
            None
        }
    }
}

pub struct EnumVariantAggregatorTimeWeight {
    sum: f32,
}

impl AggregatorTimeWeight<EnumVariant> for EnumVariantAggregatorTimeWeight {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EnumVariant) {
        let f = dt.ns() as f32 / bl.ns() as f32;
        eprintln!("INGEST ENUM  {}  {:?}", f, val);
        self.sum += f * val.ix() as f32;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = f32::identity_sum();
    }

    fn result_and_reset_for_new_bin(&mut self) -> <EnumVariant as EventValueType>::AggTimeWeightOutputAvg {
        let ret = self.sum.clone();
        self.sum = f32::identity_sum();
        ret
    }
}

impl EventValueType for EnumVariant {
    type Container = EnumVariantContainer;
    type AggregatorTimeWeight = EnumVariantAggregatorTimeWeight;
    type AggTimeWeightOutputAvg = f32;

    // TODO remove this from trait, only needed for common numeric cases but not in general.
    fn identity_sum() -> Self {
        todo!()
    }

    // TODO also remove from trait, push it to a more specialized trait for the plain numeric cases.
    fn add_weighted(&self, add: &Self, f: f32) -> Self {
        todo!()
    }
}
