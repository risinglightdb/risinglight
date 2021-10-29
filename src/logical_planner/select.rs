//! Logical planner of `select` statement.
//!
//! A `select` statement will be planned to a compose of:
//!
//! - [`LogicalSeqScan`] (from *) or dummy plan (no from)
//! - [`LogicalFilter`] (where *)
//! - [`LogicalProjection`] (select *)
//! - [`LogicalOrder`] (order by *)
use super::*;
use crate::binder::{BoundSelect, BoundTableRef};

impl LogicalPlaner {
    pub fn plan_select(&self, stmt: Box<BoundSelect>) -> Result<LogicalPlan, LogicalPlanError> {
        let mut plan = LogicalPlan::Dummy;
        if let Some(table_ref) = stmt.from_table.get(0) {
            plan = self.plan_table_ref(table_ref)?;
        }

        if let Some(expr) = stmt.where_clause {
            plan = LogicalPlan::Filter(LogicalFilter {
                expr,
                child: Box::new(plan),
            });
        }

        // TODO: support the following clauses
        assert_eq!(stmt.limit, None, "TODO: plan limit");
        assert_eq!(stmt.offset, None, "TODO: plan offset");
        assert!(!stmt.select_distinct, "TODO: plan distinct");

        if !stmt.select_list.is_empty() {
            plan = LogicalPlan::Projection(LogicalProjection {
                project_expressions: stmt.select_list,
                child: Box::new(plan),
            });
        }
        if !stmt.orderby.is_empty() {
            plan = LogicalPlan::Order(LogicalOrder {
                comparators: stmt.orderby,
                child: Box::new(plan),
            });
        }
        if plan == LogicalPlan::Dummy {
            return Err(LogicalPlanError::InvalidSQL);
        }
        Ok(plan)
    }

    pub fn plan_table_ref(
        &self,
        table_ref: &BoundTableRef,
    ) -> Result<LogicalPlan, LogicalPlanError> {
        match table_ref {
            BoundTableRef::BaseTableRef {
                ref_id,
                table_name: _,
                column_ids,
            } => Ok(LogicalPlan::SeqScan(LogicalSeqScan {
                table_ref_id: *ref_id,
                column_ids: column_ids.to_vec(),
            })),
            BoundTableRef::JoinTableRef {
                relation,
                join_tables,
            } => {
                let relation_plan = self.plan_table_ref(relation)?;
                let mut join_table_plans = vec![];
                for table in join_tables.iter() {
                    let table_plan = self.plan_table_ref(&table.table_ref)?;
                    join_table_plans.push(LogicalJoinTable {
                        table_plan: Box::new(table_plan),
                        join_op: table.join_op.clone(),
                    });
                }
                Ok(LogicalPlan::Join(LogicalJoin {
                    relation_plan: Box::new(relation_plan),
                    join_table_plans,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::*;
    use crate::catalog::*;
    use crate::parser::parse;
    use crate::types::{DataTypeExt, DataTypeKind};
    use std::sync::Arc;
    /*
    #[test]
    fn plan_select() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table(
                "t".into(),
                vec![
                    ColumnCatalog::new(0, "a".into(), DataTypeKind::Int.not_null().to_column()),
                    ColumnCatalog::new(1, "b".into(), DataTypeKind::Int.not_null().to_column()),
                ],
                false,
            )
            .unwrap();

        let sql = "select b, a from t";
        let stmts = parse(sql).unwrap();
        let stmt = binder.bind(&stmts[0]).unwrap();
        let planner = LogicalPlaner::default();
        let plan = planner.plan(stmt).unwrap();
        assert_eq!(
            plan,
            LogicalPlan::Projection(LogicalProjection {
                project_expressions: vec![
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            table_name: "t".to_string(),
                            column_ref_id: ColumnRefId::new(0, 0, 0, 1),
                            column_index: 0,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            table_name: "t".to_string(),
                            column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                            column_index: 1,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                ],
                child: Box::new(LogicalPlan::SeqScan(LogicalSeqScan {
                    table_ref_id: TableRefId {
                        database_id: 0,
                        schema_id: 0,
                        table_id: 0
                    },
                    column_ids: vec![1, 0],
                })),
            })
        )
    }*/
}
