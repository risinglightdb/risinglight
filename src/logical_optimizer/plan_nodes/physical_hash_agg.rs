use std::fmt;

use super::*;
use crate::binder::{BoundAggCall, BoundExpr};

/// The physical plan of hash aggregation.
#[derive(Debug, Clone)]
pub struct PhysicalHashAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl_plan_node!(PhysicalHashAgg, [child]);

impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg: {} agg calls", self.agg_calls.len(),)
    }
}
