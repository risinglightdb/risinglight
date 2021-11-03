use super::*;
use crate::binder::{BoundExprKind, BoundJoinOperator, BoundTableRef};
use crate::parser::{Query, SelectItem, SetExpr};

/// A bound `select` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    // TODO: aggregates: Vec<BoundExpr>,
    pub from_table: Vec<BoundTableRef>,
    pub where_clause: Option<BoundExpr>,
    pub select_distinct: bool,
    // pub groupby: Option<BoundGroupBy>,
    pub orderby: Vec<BoundOrderBy>,
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
        // TODO: process group-by
        let mut where_clause = match &select.selection {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };
        let mut orderby = vec![];
        for e in query.order_by.iter() {
            orderby.push(BoundOrderBy {
                expr: self.bind_expr(&e.expr)?,
                descending: e.asc == Some(false),
            });
        }
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
            match item {
                SelectItem::UnnamedExpr(expr) => select_list.push(self.bind_expr(expr)?),
                SelectItem::ExprWithAlias { expr, .. } => select_list.push(self.bind_expr(expr)?),
                SelectItem::Wildcard => {
                    select_list.extend_from_slice(self.bind_all_column_refs()?.as_slice())
                }
                _ => todo!("bind select list"),
            };
            // return_names.push(expr.get_name());
        }
        // TODO: move the column index binding into phyiscal planner
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
        if let Some(expr) = &mut where_clause {
            self.bind_column_idx_for_expr(&mut expr.kind);
        }
        for orderby in orderby.iter_mut() {
            self.bind_column_idx_for_expr(&mut orderby.expr.kind);
        }
        for table_ref in from_table.iter_mut() {
            self.bind_column_idx_for_table(table_ref);
        }

        Ok(Box::new(BoundSelect {
            select_list,
            from_table,
            where_clause,
            select_distinct: select.distinct,
            orderby,
            limit,
            offset,
        }))
    }

    pub(super) fn bind_column_ids(&mut self, table_ref: &mut BoundTableRef) {
        match table_ref {
            BoundTableRef::BaseTableRef {
                ref_id: _,
                table_name,
                column_ids,
            } => {
                *column_ids = self.context.column_ids.get(table_name).unwrap().clone();
            }
            BoundTableRef::JoinTableRef {
                relation,
                join_tables,
            } => {
                self.bind_column_ids(relation);
                for table in join_tables.iter_mut() {
                    self.bind_column_ids(&mut table.table_ref);
                }
            }
        }
    }

    pub(super) fn bind_column_idx_for_table(&mut self, table_ref: &mut BoundTableRef) {
        if let BoundTableRef::JoinTableRef {
            relation: _,
            join_tables,
        } = table_ref
        {
            for table in join_tables.iter_mut() {
                match &mut table.join_op {
                    BoundJoinOperator::Inner(constraint) => match constraint {
                        BoundJoinConstraint::On(expr) => {
                            self.bind_column_idx_for_expr(&mut expr.kind);
                        }
                    },
                }
            }
        }
    }

    pub(super) fn bind_column_idx_for_expr(&mut self, expr_kind: &mut BoundExprKind) {
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

/// A bound `order by` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundOrderBy {
    pub expr: BoundExpr,
    pub descending: bool,
}
