use super::*;
use crate::binder::BoundTableRef;
use crate::parser::{Query, SelectItem, SetExpr};

/// A bound `select` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    pub from_table: Option<BoundTableRef>,
    pub where_clause: Option<BoundExpr>,
    pub select_distinct: bool,
    pub group_by: Vec<BoundExpr>,
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
        let mut from_table = if select.from.is_empty() {
            None
        } else if select.from.len() == 1 {
            Some(self.bind_table_with_joins(&select.from[0])?)
        } else {
            // Bind cross join
            let relation = self.bind_table_ref(&select.from[0].relation)?;
            assert!(select.from[0].joins.is_empty());
            let mut join_tables = vec![];
            for table_with_join in &select.from[1..] {
                let join_table = self.bind_table_ref(&table_with_join.relation)?;
                assert!(table_with_join.joins.is_empty());
                let join_op = BoundJoinOperator::CrossJoin;
                let join_ref = BoundedSingleJoinTableRef {
                    table_ref: (join_table.into()),
                    join_op,
                };
                join_tables.push(join_ref);
            }
            Some(BoundTableRef::JoinTableRef {
                relation: (relation.into()),
                join_tables,
            })
        };

        let where_clause = match &select.selection {
            Some(expr) => Some(self.bind_expr(expr)?),
            None => None,
        };
        let mut orderby = vec![];
        for e in &query.order_by {
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
        let mut group_by = vec![];
        for group_key in &select.group_by {
            group_by.push(self.bind_expr(group_key)?);
        }

        // Bind the select list.
        let mut select_list = vec![];
        // let mut return_names = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr, .. } => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::Wildcard => {
                    select_list.extend_from_slice(self.bind_all_column_refs()?.as_slice())
                }
                _ => todo!("bind select list"),
            };
            // return_names.push(expr.get_name());
        }

        // Add referred columns for base table reference
        if let Some(table_ref) = &mut from_table {
            self.bind_column_ids(table_ref);
        }

        Ok(Box::new(BoundSelect {
            select_list,
            from_table,
            where_clause,
            select_distinct: select.distinct,
            group_by,
            orderby,
            limit,
            offset,
        }))
    }

    pub fn bind_column_ids(&self, table_ref: &mut BoundTableRef) {
        match table_ref {
            BoundTableRef::BaseTableRef {
                table_name,
                column_ids,
                column_descs,
                ..
            } => {
                *column_ids = self.context.column_ids.get(table_name).unwrap().clone();
                *column_descs = self.context.column_descs.get(table_name).unwrap().clone();
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
}

/// A bound `order by` statement.
#[derive(PartialEq, Clone)]
pub struct BoundOrderBy {
    pub expr: BoundExpr,
    pub descending: bool,
}

impl std::fmt::Debug for BoundOrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} ({})",
            self.expr,
            if self.descending { "desc" } else { "asc" }
        )
    }
}
