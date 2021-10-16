use super::*;
use crate::binder::{BoundExprKind, BoundJoinOperator, BoundTableRef};
use crate::parser::{Query, SelectItem, SetExpr};

#[derive(Debug, PartialEq, Clone)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    // TODO: aggregates: Vec<BoundExpr>,
    pub from_table: Vec<BoundTableRef>,
    pub where_clause: Option<BoundExpr>,
    pub select_distinct: bool,
    // pub groupby: Option<BoundGroupBy>,
    // pub orderby: Option<BoundOrderBy>,
    pub limit: Option<BoundExpr>,
    pub offset: Option<BoundExpr>,
    // pub return_names: Vec<String>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<Box<BoundSelect>, BindError> {
        self.push_context();
        let ret = self.bind_select_internal(query);
        self.pop_context();
        ret
    }

    fn bind_select_internal(&mut self, query: &Query) -> Result<Box<BoundSelect>, BindError> {
        let select = match &query.body {
            SetExpr::Select(select) => &**select,
            _ => todo!("not select"),
        };
        // Bind table ref
        let mut from_table = vec![];
        // We don't support cross join now.
        // The cross join will have multiple TableWithJoin in "from" struct.
        // Other types of join will onyl have one TableWithJoin in "from" struct.
        assert!(select.from.len() <= 1);

        for table_with_join in select.from.iter() {
            let table_ref = self.bind_table_with_joins(table_with_join)?;
            from_table.push(table_ref);
        }
        // TODO: process where, order by, group-by
        let mut where_clause = match &select.selection {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };
        let limit = match &query.limit {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };
        let offset = match &query.offset {
            Some(offset) => Some(self.bind_expr(&offset.value)?),
            None => None,
        };

        // Bind the select list.
        // we only support column reference now
        let mut select_list = vec![];
        // let mut return_names = vec![];
        for item in select.projection.iter() {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) => self.bind_expr(expr)?,
                SelectItem::ExprWithAlias { expr, .. } => self.bind_expr(expr)?,
                _ => todo!("bind select list"),
            };
            // return_names.push(expr.get_name());
            select_list.push(expr);
        }

        // Add referred columns for base table reference
        for table_ref in from_table.iter_mut() {
            self.bind_column_ids(table_ref);
        }

        // Do it again, we need column index!
        self.column_sum_count = vec![0];

        for base_table_name in self.base_table_refs.iter() {
            let idxs = self.context.column_ids.get_mut(base_table_name).unwrap();
            self.column_sum_count
                .push(idxs.len() + self.column_sum_count[self.column_sum_count.len() - 1]);
        }

        for expr in select_list.iter_mut() {
            self.bind_column_idx_for_expr(&mut expr.kind);
        }

        match &mut where_clause {
            Some(expr) => self.bind_column_idx_for_expr(&mut expr.kind),
            None => {}
        };

        for table_ref in from_table.iter_mut() {
            self.bind_column_idx_for_table(table_ref);
        }

        Ok(Box::new(BoundSelect {
            select_list,
            from_table,
            where_clause,
            select_distinct: select.distinct,
            limit,
            offset,
        }))
    }

    fn bind_column_ids(&mut self, table_ref: &mut BoundTableRef) {
        match table_ref {
            BoundTableRef::BaseTableRef {
                ref_id: _,
                table_name,
                column_ids,
            } => {
                *column_ids = self.context.column_ids.get(table_name).unwrap().clone();
            }
            BoundTableRef::JoinTableRef {
                left_table,
                right_table,
                join_op: _,
            } => {
                self.bind_column_ids(left_table);
                self.bind_column_ids(right_table);
            }
            _ => {}
        }
    }

    fn bind_column_idx_for_table(&mut self, table_ref: &mut BoundTableRef) {
        if let BoundTableRef::JoinTableRef {
            left_table: _,
            right_table: _,
            join_op,
        } = table_ref
        {
            match join_op {
                BoundJoinOperator::Inner(constraint) => match constraint {
                    BoundJoinConstraint::On(expr) => {
                        self.bind_column_idx_for_expr(&mut expr.kind);
                    }
                },
            }
        }
    }

    fn bind_column_idx_for_expr(&mut self, expr_kind: &mut BoundExprKind) {
        match expr_kind {
            BoundExprKind::ColumnRef(col_ref) => {
                let table_idx = self
                    .base_table_refs
                    .iter()
                    .position(|r| r.eq(&col_ref.table_name))
                    .unwrap();
                let column_ids = self.context.column_ids.get(&col_ref.table_name).unwrap();
                let idx = column_ids
                    .iter()
                    .position(|idx| *idx == col_ref.column_ref_id.column_id)
                    .unwrap();
                col_ref.column_index = (self.column_sum_count[table_idx] + idx) as u32;
                println!("Column index: {}", col_ref.column_index);
            }
            BoundExprKind::BinaryOp(bin_op) => {
                self.bind_column_idx_for_expr(&mut bin_op.left_expr.kind);
                self.bind_column_idx_for_expr(&mut bin_op.right_expr.kind);
            }
            BoundExprKind::UnaryOp(unary_op) => {
                self.bind_column_idx_for_expr(&mut unary_op.expr.kind);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnRefId, RootCatalog};
    use crate::parser::{parse, BinaryOperator, Statement};
    use crate::types::{DataType, DataTypeExt, DataTypeKind};
    use std::sync::Arc;

    #[test]
    fn bind_join() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table(
                "x".into(),
                vec![
                    ColumnCatalog::new(0, "a".into(), DataTypeKind::Int.not_null().to_column()),
                    ColumnCatalog::new(1, "b".into(), DataTypeKind::Int.not_null().to_column()),
                ],
                false,
            )
            .unwrap();
        schema
            .add_table(
                "y".into(),
                vec![
                    ColumnCatalog::new(0, "c".into(), DataTypeKind::Int.not_null().to_column()),
                    ColumnCatalog::new(1, "d".into(), DataTypeKind::Int.not_null().to_column()),
                ],
                false,
            )
            .unwrap();

        let sql = "select c, d, a, b from x join y on a = c;";
        let stmts = parse(sql).unwrap();
        let query = match &stmts[0] {
            Statement::Query(q) => &*q,
            _ => panic!("type mismatch"),
        };
        assert_eq!(
            *binder.bind_select(query).unwrap(),
            BoundSelect {
                select_list: vec![
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            table_name: "y".into(),
                            column_ref_id: ColumnRefId::new(0, 0, 1, 0),
                            column_index: 2,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            table_name: "y".into(),
                            column_ref_id: ColumnRefId::new(0, 0, 1, 1),
                            column_index: 3,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            table_name: "x".into(),
                            column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                            column_index: 0,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                    BoundExpr {
                        kind: BoundExprKind::ColumnRef(BoundColumnRef {
                            table_name: "x".into(),
                            column_ref_id: ColumnRefId::new(0, 0, 0, 1),
                            column_index: 1,
                        }),
                        return_type: Some(DataTypeKind::Int.not_null()),
                    },
                ],
                from_table: vec![BoundTableRef::JoinTableRef {
                    left_table: Box::new(BoundTableRef::BaseTableRef {
                        ref_id: TableRefId {
                            database_id: 0,
                            schema_id: 0,
                            table_id: 0
                        },
                        table_name: "x".into(),
                        column_ids: vec![0, 1]
                    }),
                    right_table: Box::new(BoundTableRef::BaseTableRef {
                        ref_id: TableRefId {
                            database_id: 0,
                            schema_id: 0,
                            table_id: 1
                        },
                        table_name: "y".into(),
                        column_ids: vec![0, 1]
                    }),
                    join_op: BoundJoinOperator::Inner(BoundJoinConstraint::On(BoundExpr {
                        kind: BoundExprKind::BinaryOp(BoundBinaryOp {
                            left_expr: Box::new(BoundExpr {
                                kind: BoundExprKind::ColumnRef(BoundColumnRef {
                                    table_name: "x".into(),
                                    column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                                    column_index: 0,
                                }),
                                return_type: Some(DataType {
                                    kind: DataTypeKind::Int,
                                    nullable: false
                                })
                            }),
                            op: BinaryOperator::Eq,
                            right_expr: Box::new(BoundExpr {
                                kind: BoundExprKind::ColumnRef(BoundColumnRef {
                                    table_name: "y".into(),
                                    column_ref_id: ColumnRefId::new(0, 0, 1, 0),
                                    column_index: 2,
                                }),
                                return_type: Some(DataType {
                                    kind: DataTypeKind::Int,
                                    nullable: false
                                })
                            }),
                        }),
                        return_type: Some(DataType {
                            kind: DataTypeKind::Int,
                            nullable: false
                        })
                    }))
                }],
                where_clause: None,
                select_distinct: false,
                limit: None,
                offset: None,
            }
        );
    }
}
