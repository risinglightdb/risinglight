// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.
use serde::Serialize;

use super::BoundExpr::*;
use super::{BoundExpr, BoundTableRef, *};
use crate::parser::{Query, SelectItem, SetExpr};
use crate::types::DataValue::Bool;

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
    pub having: Option<BoundExpr>,
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
        let select = match &*query.body {
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
                let join_ref = BoundedSingleJoinTableRef {
                    table_ref: (join_table.into()),
                    join_op: BoundJoinOperator::Inner,
                    join_cond: Constant(Bool(true)),
                };
                join_tables.push(join_ref);
            }
            Some(BoundTableRef::JoinTableRef {
                relation: (relation.into()),
                join_tables,
            })
        };

        let where_clause = select
            .selection
            .as_ref()
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        let offset = query
            .offset
            .as_ref()
            .map(|offset| self.bind_expr(&offset.value))
            .transpose()?;

        // Bind the select list.
        let mut select_list = vec![];
        // let mut return_names = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.bind_expr(expr)?;
                    let expr = self.bind_alias(expr, alias.clone());
                    select_list.push(expr);
                }
                SelectItem::Wildcard => {
                    select_list.extend_from_slice(self.bind_all_column_refs()?.as_slice())
                }
                _ => todo!("bind select list"),
            };
            // return_names.push(expr.get_name());
        }

        let mut group_by = vec![];
        for group_key in &select.group_by {
            group_by.push(self.bind_expr(group_key)?);
        }

        let mut having = None;
        if let Some(expr) = select.having.as_ref() {
            having = Some(self.bind_expr(expr)?);
        }

        let mut orderby = vec![];
        for e in &query.order_by {
            orderby.push(BoundOrderBy {
                expr: self.bind_expr(&e.expr)?,
                descending: e.asc == Some(false),
            });
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
            having,
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
#[derive(PartialEq, Clone, Serialize)]
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
