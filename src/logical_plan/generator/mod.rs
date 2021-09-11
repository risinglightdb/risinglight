use super::*;
use crate::parser::Expression;
use crate::parser::{CreateTableStmt, InsertStmt, SQLStatement, SelectStmt, TableRef};

pub struct LogicalPlanGenerator {}

impl LogicalPlanGenerator {
    pub fn new() -> LogicalPlanGenerator {
        LogicalPlanGenerator {}
    }

    pub fn generate_logical_plan(
        &self,
        sql: &SQLStatement,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match sql {
            SQLStatement::CreateTable(create_table_stmt) => {
                self.generate_create_table_logical_plan(create_table_stmt)
            }
            SQLStatement::Insert(insert_stmt) => self.generate_insert_logical_plan(insert_stmt),
            SQLStatement::Select(select_stmt) => self.generate_select_logical_plan(select_stmt),
            _ => Err(LogicalPlanError::InvalidSQL),
        }
    }

    pub fn generate_create_table_logical_plan(
        &self,
        stmt: &CreateTableStmt,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let plan = CreateTableLogicalPlan {
            database_id: stmt.database_id.unwrap(),
            schema_id: stmt.schema_id.unwrap(),
            table_name: stmt.table_name.clone(),
            column_descs: stmt.column_descs.clone(),
        };
        Ok(LogicalPlan::CreateTable(plan))
    }

    pub fn generate_insert_logical_plan(
        &self,
        stmt: &InsertStmt,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let plan = InsertLogicalPlan {
            table_ref_id: stmt.table_ref_id.unwrap(),
            column_ids: stmt.column_ids.clone(),
            values: stmt.values.clone(),
        };
        Ok(LogicalPlan::Insert(plan))
    }

    pub fn generate_select_logical_plan(
        &self,
        stmt: &SelectStmt,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        let mut plan = LogicalPlan::Dummy;
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
        if plan == LogicalPlan::Dummy {
            return Err(LogicalPlanError::InvalidSQL);
        }

        Ok(plan)
    }

    pub fn generate_table_ref_plan(
        &self,
        table_ref: &TableRef,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match table_ref {
            TableRef::Base(base_ref) => Ok(LogicalPlan::SeqScan(SeqScanLogicalPlan::new(
                base_ref.table_ref_id.as_ref().unwrap(),
                &base_ref.column_ids,
            ))),
            _ => Err(LogicalPlanError::InvalidSQL),
        }
    }

    pub fn generate_projection_plan(
        &self,
        exprs: &Vec<Expression>,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalPlan::Projection(ProjectionLogicalPlan {
            project_expressions: exprs.to_vec(),
            child: Box::new(plan),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::{Bind, Binder};
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRefId, RootCatalog, TableRefId};
    use crate::parser::{ColumnRef, ExprKind, Expression, SQLStatement};
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
                vec![
                    ColumnCatalog::new(
                        0,
                        "a".into(),
                        ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ),
                    ColumnCatalog::new(
                        1,
                        "b".into(),
                        ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ),
                ],
                false,
            )
            .unwrap();

        let sql = "select a, b from t; ";
        let mut stmts = SQLStatement::parse(sql).unwrap();
        stmts[0].bind(&mut binder).unwrap();
        let planner = LogicalPlanGenerator::new();
        let plan = planner.generate_logical_plan(&stmts[0]).unwrap();
        assert_eq!(
            plan,
            LogicalPlan::Projection(ProjectionLogicalPlan {
                project_expressions: vec![
                    Expression {
                        alias: None,
                        return_type: Some(DataType::new(DataTypeKind::Int32, false)),
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
                        return_type: Some(DataType::new(DataTypeKind::Int32, false)),
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
                child: Box::new(LogicalPlan::SeqScan(SeqScanLogicalPlan::new(
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
