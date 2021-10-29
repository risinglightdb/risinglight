//! Logical planner of `select` statement.
//!
//! A `select` statement will be planned to a compose of:
//!
//! - [`LogicalSeqScan`] (from *) or dummy plan (no from)
//! - [`LogicalFilter`] (where *)
//! - [`LogicalProjection`] (select *)
//! - [`LogicalOrder`] (order by *)

use super::*;
use crate::binder::{BoundExprKind, BoundSelect, BoundTableRef};

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
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = match stmt.limit {
                Some(limit) => match limit.kind {
                    BoundExprKind::Constant(v) => v.as_usize()?.unwrap_or(usize::MAX / 2),
                    _ => panic!("limit only support constant expression"),
                },
                None => usize::MAX / 2, // avoid 'offset + limit' overflow
            };
            let offset = match stmt.offset {
                Some(offset) => match offset.kind {
                    BoundExprKind::Constant(v) => v.as_usize()?.unwrap_or(0),
                    _ => panic!("offset only support constant expression"),
                },
                None => 0,
            };
            plan = LogicalPlan::Limit(LogicalLimit {
                offset,
                limit,
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
                left_table,
                right_table,
                join_op,
            } => {
                let left_plan = self.plan_table_ref(left_table)?;
                let right_plan = self.plan_table_ref(right_table)?;
                Ok(LogicalPlan::Join(LogicalJoin {
                    left_plan: Box::new(left_plan),
                    right_plan: Box::new(right_plan),
                    join_op: join_op.clone(),
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
    }
}
