use std::fmt;

use super::*;
use crate::catalog::TableRefId;

/// The physical plan of `DELETE`.
#[derive(Debug, Clone)]
pub struct PhysicalDelete {
    logical: LogicalDelete,
}

impl PlanTreeNodeUnary for PhysicalFilter {
    fn child(&self) -> PlanRef {
        self.logical.child()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.logcial().clone_with_child(child))
    }
}
impl_plan_tree_node_for_unary!(PhysicalFilter);
impl PlanNode for PhysicalDelete {}
impl fmt::Display for PhysicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDelete: table {}", self.table_ref_id.table_id)
    }
}
