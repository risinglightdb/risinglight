use std::fmt;

use super::*;
use crate::binder::{BoundAggCall, BoundExpr};

/// The physical plan of hash aggregation.
#[derive(Debug, Clone)]
pub struct PhysicalHashAgg {
    logcial: LogicalAggregate,
}

impl PhysicalHashAgg {
    pub fn new(logcial: LogicalAggregate) -> Self {
        Self { logcial }
    }

    /// Get a reference to the physical hash agg's logcial.
    pub fn logcial(&self) -> &LogicalAggregate {
        &self.logcial
    }
}
impl PlanTreeNodeUnary for PhysicalHashAgg {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logcial().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalHashAgg);
impl PlanNode for PhysicalHashAgg {
    fn out_types(&self) -> Vec<DataType> {
        self.logcial.out_types()
    }
}
impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg: {} agg calls", self.agg_calls.len(),)
    }
}
