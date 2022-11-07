// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::parser::{Expr, Query, SelectItem, SetExpr};
use crate::planner::ColumnIndexResolver;

impl Binder {
    pub(super) fn bind_query(&mut self, query: Query) -> Result {
        self.push_context();
        let ret = self.bind_query_internal(query);
        self.pop_context();
        ret
    }

    fn bind_query_internal(&mut self, query: Query) -> Result {
        let child = match *query.body {
            SetExpr::Select(select) => self.bind_select(*select, query.order_by)?,
            SetExpr::Values(values) => self.bind_values(values)?,
            _ => todo!("handle query ???"),
        };
        let limit = match query.limit {
            Some(expr) => self.bind_expr(expr)?,
            None => self.egraph.add(Node::null()),
        };
        let offset = match query.offset {
            Some(offset) => self.bind_expr(offset.value)?,
            None => self.egraph.add(Node::zero()),
        };
        Ok(self.egraph.add(Node::Limit([limit, offset, child])))
    }

    fn bind_select(&mut self, select: Select, order_by: Vec<OrderByExpr>) -> Result {
        let from = self.bind_from(select.from)?;

        let where_ = self.bind_condition(select.selection)?;

        let projection = self.bind_projection(select.projection, from)?;

        let distinct = match select.distinct {
            // TODO: distinct on
            true => projection,
            false => self.egraph.add(Node::List([].into())),
        };

        let group_list: Box<[Id]> = (select.group_by.into_iter())
            .map(|key| self.bind_expr(key))
            .try_collect()?;
        let has_groupby = !group_list.is_empty();
        let groupby = self.egraph.add(Node::List(group_list));

        let having = self.bind_condition(select.having)?;

        let orderby = self.bind_orderby(order_by)?;

        // extract aggregations from expressions
        let to_extract = self
            .egraph
            .add(Node::List([projection, distinct, having, orderby].into()));
        let aggs = self.egraph[to_extract].data.aggs.clone()?;

        let mut plan = self.egraph.add(Node::Filter([where_, from]));
        // insert agg node
        if !aggs.is_empty() || has_groupby {
            let mut list: Vec<_> = aggs.into_iter().map(|agg| self.egraph.add(agg)).collect();
            // make sure the order of the aggs is deterministic
            list.sort();
            list.dedup();
            let aggs = self.egraph.add(Node::List(list.into()));
            plan = self.egraph.add(Node::Agg([aggs, groupby, plan]));

            // check whether expressions can be composed by aggregations.
            let schema = self.egraph.add(Node::List(self.schema(plan).into()));
            let mut resolver = ColumnIndexResolver::new(&self.recexpr(schema));
            let resolved = resolver.resolve(&self.recexpr(to_extract));
            for expr in resolved.as_ref() {
                if let Node::Column(cid) = expr {
                    let name = self.catalog.get_column(cid).unwrap().name().to_string();
                    return Err(BindError::AggError(AggError::ColumnNotInAgg(name)));
                }
            }
        }
        plan = self.egraph.add(Node::Distinct([distinct, plan]));
        plan = self.egraph.add(Node::Order([orderby, plan]));
        plan = self.egraph.add(Node::Proj([projection, plan]));
        Ok(plan)
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
                    select_list.append(&mut self.schema(from));
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

    fn bind_orderby(&mut self, order_by: Vec<OrderByExpr>) -> Result {
        let mut orderby = Vec::with_capacity(order_by.len());
        for e in order_by {
            let expr = self.bind_expr(e.expr)?;
            let key = self.egraph.add(match e.asc {
                Some(true) | None => Node::Asc(expr),
                Some(false) => Node::Desc(expr),
            });
            orderby.push(key);
        }
        Ok(self.egraph.add(Node::List(orderby.into())))
    }

    fn bind_values(&mut self, Values(values): Values) -> Result {
        let mut bound_values = Vec::with_capacity(values.len());
        if values.is_empty() {
            return Ok(self.egraph.add(Node::Values([].into())));
        }

        let column_len = values[0].len();
        for row in values {
            if row.len() != column_len {
                return Err(BindError::InvalidExpression(
                    "VALUES lists must all be the same length".into(),
                ));
            }
            let mut bound_row = Vec::with_capacity(column_len);
            for expr in row {
                bound_row.push(self.bind_expr(expr)?);
            }
            bound_values.push(self.egraph.add(Node::List(bound_row.into())));
        }
        let id = self.egraph.add(Node::Values(bound_values.into()));
        self.check_type(id)?;
        Ok(id)
    }
}
