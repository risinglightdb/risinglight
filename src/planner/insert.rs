use super::*;
use crate::catalog::TableRefId;
use crate::parser::{Expression, InsertStmt};
use crate::types::ColumnId;
use std::convert::TryFrom;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertPlan {
    table_ref_id: TableRefId,
    column_ids: Vec<ColumnId>,
    values_: Vec<Vec<Expression>>,
}

impl TryFrom<&InsertStmt> for InsertPlan {
    type Error = PlanError;

    fn try_from(insert_stmt: &InsertStmt) -> Result<InsertPlan, PlanError> {
        let mut plan = InsertPlan {
            table_ref_id: insert_stmt.table_ref_id.unwrap(),
            column_ids: insert_stmt.column_ids.clone(),
            values_: vec![],
        };

        for val in insert_stmt.values.iter() {
            plan.values_.push(val.to_vec());
        }

        Ok(plan)
    }
}
