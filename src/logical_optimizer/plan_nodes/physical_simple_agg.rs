use std::fmt;

use super::*;
use crate::binder::BoundAggCall;

/// The physical plan of simple aggregation.
#[derive(Debug, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: PlanRef,
}

impl_plan_tree_node!(PhysicalSimpleAgg, [child]);
impl PlanNode for PhysicalSimpleAgg {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for agg in &mut self.agg_calls {
            for arg in &mut agg.args {
                rewriter.rewrite_expr(arg);
            }
        }
    }
}

impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalSimpleAgg: {} agg calls", self.agg_calls.len(),)
    }
}
