use crate::{
    catalog::TableRefId,
    logical_optimizer::plan_nodes::{LogicalPlan, UnaryLogicalPlanNode},
    types::ColumnId,
};

use super::LogicalPlanRef;

/// The logical plan of `INSERT`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: LogicalPlanRef,
}

impl UnaryLogicalPlanNode for LogicalInsert {
    fn child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalInsert(LogicalInsert {
            child,
            table_ref_id: self.table_ref_id,
            column_ids: self.column_ids.clone(),
        })
        .into()
    }
}
