use super::*;
use crate::catalog::TableRefId;
use crate::parser::{Expression, InsertStmt};
use crate::types::ColumnId;
use std::convert::TryFrom;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertLogicalPlan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub values: Vec<Vec<Expression>>,
}

impl TryFrom<&InsertStmt> for InsertLogicalPlan {
    type Error = LogicalPlanError;

    fn try_from(insert_stmt: &InsertStmt) -> Result<InsertLogicalPlan, LogicalPlanError> {
        let plan = InsertLogicalPlan {
            table_ref_id: insert_stmt.table_ref_id.unwrap(),
            column_ids: insert_stmt.column_ids.clone(),
            values: insert_stmt.values.clone(),
        };
        Ok(plan)
    }
}
