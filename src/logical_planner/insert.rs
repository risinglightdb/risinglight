use super::*;
use crate::binder::{BoundExpr, BoundInsert};
use crate::catalog::TableRefId;
use crate::logical_optimizer::plan_node::UnaryLogicalPlanNode;
use crate::types::{ColumnId, DataType};

/// The logical plan of `INSERT`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: LogicalPlanRef,
}

/// The logical plan of `VALUES`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl UnaryLogicalPlanNode for LogicalInsert {
    fn get_child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalInsert(LogicalInsert {
            child,
            table_ref_id: self.table_ref_id,
            column_ids: self.column_ids.clone(),
        })
        .into()
    }
}

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::LogicalInsert(LogicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            child: LogicalPlan::LogicalValues(LogicalValues {
                column_types: stmt.column_types,
                values: stmt.values,
            })
            .into(),
        }))
    }
}
