use std::fmt;

use itertools::Itertools;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode};
use crate::catalog::TableRefId;
use crate::logical_optimizer::plan_nodes::UnaryPlanNode;
use crate::types::ColumnId;

/// The logical plan of `INSERT`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: PlanRef,
}

impl UnaryPlanNode for LogicalInsert {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalInsert(LogicalInsert {
            child,
            table_ref_id: self.table_ref_id,
            column_ids: self.column_ids.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {LogicalInsert}

impl fmt::Display for LogicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalInsert: table {}, columns [{}]",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", ")
        )
    }
}
