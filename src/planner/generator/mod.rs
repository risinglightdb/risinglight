use super::*;
use crate::parser::{CreateTableStmt, InsertStmt, SQLStatement, SelectStmt, TableRef};
use crate::parser::{Expression};
use std::convert::TryFrom;
use std::sync::Arc;

pub struct PlanGenerator {}

impl PlanGenerator {
    pub fn new() -> PlanGenerator {
        PlanGenerator {}
    }
    pub fn generate_plan(&self, sql: &SQLStatement) -> Result<Plan, PlanError> {
        match sql {
            SQLStatement::CreateTable(create_table_stmt) => {
                self.generate_create_table_plan(create_table_stmt)
            }
            SQLStatement::Insert(insert_stmt) => self.generate_insert_plan(insert_stmt),
            SQLStatement::Select(select_stmt) => self.generate_select_plan(select_stmt),
            _ => Err(PlanError::InvalidSQL),
        }
    }

    pub fn generate_create_table_plan(
        &self,
        create_table_stmt: &CreateTableStmt,
    ) -> Result<Plan, PlanError> {
        let mut plan = CreateTablePlan {
            database_id: create_table_stmt.database_id.unwrap(),
            schema_id: create_table_stmt.schema_id.unwrap(),
            table_name: create_table_stmt.table_name.clone(),
            column_descs: vec![],
        };

        for desc in create_table_stmt.column_descs.iter() {
            plan.column_descs.push(desc.clone());
        }

        Ok(Plan::CreateTable(plan))
    }

    pub fn generate_insert_plan(&self, insert_stmt: &InsertStmt) -> Result<Plan, PlanError> {
        let mut plan = InsertPlan {
            table_ref_id: insert_stmt.table_ref_id.unwrap(),
            column_ids: insert_stmt.column_ids.clone(),
            values_: vec![],
        };

        for val in insert_stmt.values.iter() {
            plan.values_.push(val.to_vec());
        }

        Ok(Plan::Insert(plan))
    }

    pub fn generate_select_plan(&self, select_stmt: &SelectStmt) -> Result<Plan, PlanError> {
        let mut plan = Plan::Dummy;
        if select_stmt.from_table.is_some() {
            plan = self.generate_table_ref_plan(select_stmt.from_table.as_ref().unwrap())?;
        }

        // TODO: support the following clauses
        assert_eq!(select_stmt.where_clause, None);
        assert_eq!(select_stmt.limit, None);
        assert_eq!(select_stmt.offset, None);
        assert_eq!(select_stmt.select_distinct, false);
        
        if select_stmt.select_list.len() > 0 {
            plan = self.generate_projection_plan(&select_stmt.select_list, plan)?;
        }

        Ok(plan)
    }

    pub fn generate_table_ref_plan(&self, table_ref: &TableRef) -> Result<Plan, PlanError> {
        match table_ref {
            TableRef::Base(base_ref) => Ok(Plan::SeqScan(SeqScanPlan::new(
                base_ref.table_ref_id.as_ref().unwrap(),
                &base_ref.column_ids,
            ))),
            _ => Err(PlanError::InvalidSQL),
        }
    }

    pub fn generate_projection_plan(&self, exprs: &Vec<Expression>, plan: Plan) -> Result<Plan, PlanError> {
        Ok(Plan::Projection(ProjectionPlan{
            project_expressions: exprs.to_vec(),
            child: Arc::new(plan)
        }))
    }
}
