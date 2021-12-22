use std::fmt;

use super::*;
use crate::binder::{BoundAggCall, BoundExpr};

/// The physical plan of hash aggregation.
#[derive(Debug, Clone)]
pub struct PhysicalHashAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: PlanRef,
    data_types: Vec<DataType>,
}

impl PhysicalHashAgg {
    pub fn new(agg_calls: Vec<BoundAggCall>, group_keys: Vec<BoundExpr>, child: PlanRef) -> Self {
        let data_types = group_keys
            .iter()
            .map(|expr| expr.return_type().unwrap())
            .chain(
                agg_calls
                    .iter()
                    .map(|agg_call| agg_call.return_type.clone()),
            )
            .collect();
        PhysicalHashAgg {
            agg_calls,
            group_keys,
            child,
            data_types,
        }
    }
}

impl_plan_tree_node!(PhysicalHashAgg, [child]);
impl PlanNode for PhysicalHashAgg {
    fn rewrite_expr(&mut self, rewriter: &mut dyn Rewriter) {
        for agg in &mut self.agg_calls {
            for arg in &mut agg.args {
                rewriter.rewrite_expr(arg);
            }
        }
        for keys in &mut self.group_keys {
            rewriter.rewrite_expr(keys);
        }
    }
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}
impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg: {} agg calls", self.agg_calls.len(),)
    }
}
