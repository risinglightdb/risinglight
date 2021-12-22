use std::fmt;

use super::*;
use crate::binder::BoundAggCall;

/// The physical plan of simple aggregation.
#[derive(Debug, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: PlanRef,
    data_types: Vec<DataType>,
}

impl PhysicalSimpleAgg {
    pub fn new(agg_calls: Vec<BoundAggCall>, child: PlanRef) -> Self {
        let data_types = agg_calls
            .iter()
            .map(|agg_call| agg_call.return_type.clone())
            .collect();
        PhysicalSimpleAgg {
            agg_calls,
            child,
            data_types,
        }
    }
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
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}

impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalSimpleAgg: {} agg calls", self.agg_calls.len(),)
    }
}
