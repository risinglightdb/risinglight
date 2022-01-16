use std::fmt;

use super::*;

/// The physical plan of `EXPLAIN`.
#[derive(Debug, Clone)]
pub struct PhysicalExplain {
    logical: LogicalExplain,
}

impl PlanTreeNodeUnary for PhysicalExplain {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logcial().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalExplain);
impl PlanNode for PhysicalExplain {}
impl fmt::Display for PhysicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalExplain:")
    }
}
