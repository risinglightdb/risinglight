use std::fmt;

use super::*;
use crate::binder::BoundOrderBy;

/// The physical plan of order.
#[derive(Debug, Clone)]
pub struct PhysicalOrder {
    logical: LogicalOrder,
}

impl PlanTreeNodeUnary for PhysicalOrder {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logcial().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalOrder);
impl PlanNode for PhysicalOrder {
    fn out_types(&self) -> Vec<DataType> {
        self.child.out_types()
    }
}
impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: {:?}", self.comparators)
    }
}
