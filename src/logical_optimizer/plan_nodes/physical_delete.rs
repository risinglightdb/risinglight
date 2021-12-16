use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryPlanNode};
use crate::catalog::TableRefId;

/// The physical plan of `delete`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalDelete {
    pub table_ref_id: TableRefId,
    pub child: PlanRef,
}
impl UnaryPlanNode for PhysicalDelete {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalDelete(PhysicalDelete {
            table_ref_id: self.table_ref_id,
            child,
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalDelete}

impl fmt::Display for PhysicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDelete: table {}", self.table_ref_id.table_id)
    }
}
