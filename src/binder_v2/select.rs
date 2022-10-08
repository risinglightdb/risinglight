// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{Expr, Query, SelectItem, SetExpr};

impl Binder {
    pub(super) fn bind_query(&mut self, query: Query) -> Result {
        self.push_context();
        let ret = self.bind_query_internal(query);
        self.pop_context();
        ret
    }

    fn bind_query_internal(&mut self, query: Query) -> Result {
        let child = match *query.body {
            SetExpr::Select(select) => self.bind_select(*select)?,
            SetExpr::Values(values) => todo!("bind values"),
            _ => todo!("handle query ???"),
        };

        let mut orderby = vec![];
        for e in query.order_by {
            let expr = self.bind_expr(e.expr)?;
            let order = self.egraph.add(match e.asc {
                Some(true) | None => Node::Asc,
                Some(false) => Node::Desc,
            });
            orderby.push(self.egraph.add(Node::OrderKey([expr, order])));
        }
        let orderby = self.egraph.add(Node::List(orderby.into()));

        let limit = match query.limit {
            Some(expr) => self.bind_expr(expr)?,
            None => self.egraph.add(Node::null()),
        };
        let offset = match query.offset {
            Some(offset) => self.bind_expr(offset.value)?,
            None => self.egraph.add(Node::null()),
        };
        Ok(self.egraph.add(Node::TopN([limit, offset, orderby, child])))
    }

    pub fn bind_select(&mut self, select: Select) -> Result {
        let from = self.bind_from(select.from)?;

        let where_ = self.bind_condition(select.selection)?;

        let projection = self.bind_projection(select.projection, from)?;

        let group_list = (select.group_by.into_iter())
            .map(|key| self.bind_expr(key))
            .try_collect()?;
        let groupby = self.egraph.add(Node::List(group_list));

        let having = self.bind_condition(select.having)?;

        Ok(self
            .egraph
            .add(Node::Select([projection, from, where_, groupby, having])))
    }

    fn bind_projection(&mut self, projection: Vec<SelectItem>, from: Id) -> Result {
        let mut select_list = vec![];
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.bind_expr(expr)?;
                    self.add_alias(alias, expr)?;
                    select_list.push(expr);
                }
                SelectItem::Wildcard => {
                    let mut schema = self.egraph[from].data.schema.clone().expect("no schema");
                    select_list.append(&mut schema);
                }
                _ => todo!("bind select list"),
            }
        }
        Ok(self.egraph.add(Node::List(select_list.into())))
    }

    pub(super) fn bind_condition(&mut self, selection: Option<Expr>) -> Result {
        Ok(match selection {
            Some(expr) => self.bind_expr(expr)?,
            None => self.egraph.add(Node::true_()),
        })
    }
}
