use super::*;
use crate::catalog::TableRefId;
use crate::parser::{Expression, InsertStmt};
use crate::types::ColumnId;
use std::convert::TryFrom;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertPhysicalPlan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub values: Vec<Vec<Expression>>,
}

impl TryFrom<&InsertStmt> for InsertPhysicalPlan {
    type Error = PhysicalPlanError;

    fn try_from(insert_stmt: &InsertStmt) -> Result<InsertPhysicalPlan, PhysicalPlanError> {
        let mut plan = InsertPhysicalPlan {
            table_ref_id: insert_stmt.table_ref_id.unwrap(),
            column_ids: insert_stmt.column_ids.clone(),
            values: vec![],
        };

        for val in insert_stmt.values.iter() {
            plan.values.push(val.to_vec());
        }

        Ok(plan)
    }
}
