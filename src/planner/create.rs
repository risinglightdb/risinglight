use super::*;
use crate::catalog::ColumnCatalog;
use crate::parser::CreateTableStmt;
use crate::types::{DatabaseId, SchemaId};

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTablePlan {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub column_descs: Vec<ColumnCatalog>,
}

use std::convert::TryFrom;

impl TryFrom<&CreateTableStmt> for CreateTablePlan {
    type Error = PlanError;
    fn try_from(create_table_stmt: &CreateTableStmt) -> Result<CreateTablePlan, PlanError> {
        let mut plan = CreateTablePlan {
            database_id: create_table_stmt.database_id.unwrap(),
            schema_id: create_table_stmt.schema_id.unwrap(),
            table_name: create_table_stmt.table_name.clone(),
            column_descs: vec![],
        };

        for desc in create_table_stmt.column_descs.iter() {
            plan.column_descs.push(desc.clone());
        }

        Ok(plan)
    }
}
