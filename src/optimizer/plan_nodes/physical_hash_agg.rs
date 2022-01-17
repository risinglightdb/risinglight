use std::fmt;

use super::*;


/// The physical plan of hash aggregation.
#[derive(Debug, Clone)]
pub struct PhysicalHashAgg {
    logical: LogicalAggregate,
}

impl PhysicalHashAgg {
    pub fn new(logical: LogicalAggregate) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical hash agg's logical.
    pub fn logical(&self) -> &LogicalAggregate {
        &self.logical
    }
}
impl PlanTreeNodeUnary for PhysicalHashAgg {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logical().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalHashAgg);
impl PlanNode for PhysicalHashAgg {
    fn out_types(&self) -> Vec<DataType> {
        self.logical.out_types()
    }
}
impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalHashAgg: {} agg calls",
            self.logical().agg_calls().len(),
        )
    }
}
