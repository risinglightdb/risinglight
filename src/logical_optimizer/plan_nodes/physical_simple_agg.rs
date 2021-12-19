use std::fmt;

use super::*;
use crate::binder::{BoundAggCall};

/// The physical plan of simple aggregation.
#[derive(Debug, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: PlanRef,
}

impl_plan_node!(PhysicalSimpleAgg, [child]);

impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalSimpleAgg: {} agg calls", self.agg_calls.len(),)
    }
}
