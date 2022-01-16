use std::fmt;

use super::*;

/// The physical plan of limit operation.
#[derive(Debug, Clone)]
pub struct PhysicalLimit {
    logical: LogicalLimit,
}

impl PlanTreeNodeUnary for PhysicalLimit {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logcial().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalLimit);
impl PlanNode for PhysicalLimit {
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalLimit: offset: {}, limit: {}",
            self.offset, self.limit
        )
    }
}
