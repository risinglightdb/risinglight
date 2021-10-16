use super::*;
use crate::binder::{BoundSelect, BoundTableRef};

impl LogicalPlaner {
    pub fn plan_select(&self, stmt: Box<BoundSelect>) -> Result<LogicalPlan, LogicalPlanError> {
        let mut plan = LogicalPlan::Dummy;
        if let Some(table_ref) = stmt.from_table.get(0) {
            match table_ref {
                BoundTableRef::BaseTableRef {
                    ref_id,
                    table_name: _,
                    column_ids,
                } => {
                    plan = LogicalPlan::SeqScan(LogicalSeqScan {
                        table_ref_id: *ref_id,
                        column_ids: column_ids.to_vec(),
                    });
                }
                _ => todo!("support join"),
            }
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
        if plan == LogicalPlan::Dummy {
            return Err(LogicalPlanError::InvalidSQL);
        }
        Ok(plan)
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
