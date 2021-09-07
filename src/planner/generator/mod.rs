use super::*;
use crate::parser::Expression;
use crate::parser::{CreateTableStmt, InsertStmt, SQLStatement, SelectStmt, TableRef};
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

    pub fn generate_create_table_plan(&self, stmt: &CreateTableStmt) -> Result<Plan, PlanError> {
        let plan = CreateTablePlan {
            database_id: stmt.database_id.unwrap(),
            schema_id: stmt.schema_id.unwrap(),
            table_name: stmt.table_name.clone(),
            column_descs: stmt.column_descs.clone(),
        };
        Ok(Plan::CreateTable(plan))
    }

    pub fn generate_insert_plan(&self, stmt: &InsertStmt) -> Result<Plan, PlanError> {
        let plan = InsertPlan {
            table_ref_id: stmt.table_ref_id.unwrap(),
            column_ids: stmt.column_ids.clone(),
            values_: stmt.values.clone(),
        };
        Ok(Plan::Insert(plan))
    }

    pub fn generate_select_plan(&self, stmt: &SelectStmt) -> Result<Plan, PlanError> {
        let mut plan = Plan::Dummy;
        if stmt.from_table.is_some() {
            plan = self.generate_table_ref_plan(stmt.from_table.as_ref().unwrap())?;
        }

        // TODO: support the following clauses
        assert_eq!(stmt.where_clause, None);
        assert_eq!(stmt.limit, None);
        assert_eq!(stmt.offset, None);
        assert_eq!(stmt.select_distinct, false);

        if stmt.select_list.len() > 0 {
            plan = self.generate_projection_plan(&stmt.select_list, plan)?;
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

    pub fn generate_projection_plan(
        &self,
        exprs: &Vec<Expression>,
        plan: Plan,
    ) -> Result<Plan, PlanError> {
        Ok(Plan::Projection(ProjectionPlan {
            project_expressions: exprs.to_vec(),
            child: Arc::new(plan),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::{Bind, Binder};
    use crate::catalog::{ColumnDesc, ColumnRefId, RootCatalog, TableRefId};
    use crate::parser::{BaseTableRef, ColumnRef, ExprKind, Expression, SQLStatement};
    use crate::types::{DataType, DataTypeKind};

    use std::sync::Arc;

    #[test]
    fn generate_select() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table(
                "t".into(),
                vec!["a".into(), "b".into()],
                vec![
                    ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                ],
                false,
            )
            .unwrap();

        let sql = "select a, b from t; ";
        let mut stmts = SQLStatement::parse(sql).unwrap();
        stmts[0].bind(&mut binder).unwrap();
        let planner = PlanGenerator::new();
        let plan = planner.generate_plan(&stmts[0]).unwrap();
        assert_eq!(
            plan,
            Plan::Projection(ProjectionPlan {
                project_expressions: vec![
                    Expression {
                        alias: None,
                        // TODO: add return type when binding expression!
                        return_type: None,
                        kind: ExprKind::ColumnRef(ColumnRef {
                            table_name: Some("t".to_string()),
                            column_name: "a".to_string(),
                            column_ref_id: Some(ColumnRefId {
                                database_id: 0,
                                schema_id: 0,
                                table_id: 0,
                                column_id: 0
                            }),
                            column_index: Some(0)
                        }),
                    },
                    Expression {
                        alias: None,
                        // TODO: add return type when binding expression!
                        return_type: None,
                        kind: ExprKind::ColumnRef(ColumnRef {
                            table_name: Some("t".to_string()),
                            column_name: "b".to_string(),
                            column_ref_id: Some(ColumnRefId {
                                database_id: 0,
                                schema_id: 0,
                                table_id: 0,
                                column_id: 1
                            }),
                            column_index: Some(1)
                        }),
                    }
                ],
                child: Arc::new(Plan::SeqScan(SeqScanPlan::new(
                    &TableRefId {
                        database_id: 0,
                        schema_id: 0,
                        table_id: 0
                    },
                    &vec![0, 1]
                )))
            })
        )
    }
}
