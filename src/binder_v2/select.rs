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
        let mut projection = self.bind_projection(select.projection, from)?;
        let where_ = self.bind_where(select.selection)?;
        let groupby = self.bind_groupby(select.group_by)?;
        let having = self.bind_having(select.having)?;
        let orderby = self.bind_orderby(order_by)?;
        let distinct = match select.distinct {
            // TODO: distinct on
            true => projection,
            false => self.egraph.add(Node::List([].into())),
        };

        let mut plan = self.egraph.add(Node::Filter([where_, from]));
        plan = self.plan_agg(plan, &[projection, distinct, having, orderby], groupby)?;
        plan = self.egraph.add(Node::Filter([having, plan]));
        plan = self.plan_distinct(plan, distinct, orderby, &mut projection)?;
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

    pub(super) fn bind_where(&mut self, selection: Option<Expr>) -> Result {
        let id = self.bind_having(selection)?;
        if !self.aggs(id)?.is_empty() {
            return Err(BindError::AggError(AggError::AggInWhere));
        }
        Ok(id)
    }

    fn bind_having(&mut self, selection: Option<Expr>) -> Result {
        Ok(match selection {
            Some(expr) => self.bind_expr(expr)?,
            None => self.egraph.add(Node::true_()),
        })
    }

    fn bind_groupby(&mut self, group_by: Vec<Expr>) -> Result {
        let list = (group_by.into_iter())
            .map(|key| self.bind_expr(key))
            .try_collect()?;
        let id = self.egraph.add(Node::List(list));
        if !self.aggs(id)?.is_empty() {
            return Err(BindError::AggError(AggError::AggInGroupBy));
        }
        Ok(id)
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

    /// Extract all aggregations from `exprs` and generate an Agg plan.
    fn plan_agg(&mut self, plan: Id, exprs: &[Id], groupby: Id) -> Result {
        let exprs = self.egraph.add(Node::List(exprs.into()));
        let aggs = self.aggs(exprs)?;
        if aggs.is_empty() && self.node(groupby).as_list().is_empty() {
            return Ok(plan);
        }
        let mut list: Vec<_> = aggs.into_iter().map(|agg| self.egraph.add(agg)).collect();
        // make sure the order of the aggs is deterministic
        list.sort();
        list.dedup();
        let aggs = self.egraph.add(Node::List(list.into()));
        let plan = self.egraph.add(Node::Agg([aggs, groupby, plan]));

        // check whether the expressions can be composed by aggregations.
        let schema = self.egraph.add(Node::List(self.schema(plan).into()));
        let mut resolver = ColumnIndexResolver::new(&self.recexpr(schema));
        let resolved = resolver.resolve(&self.recexpr(exprs));
        for expr in resolved.as_ref() {
            if let Node::Column(cid) = expr {
                let name = self.catalog.get_column(cid).unwrap().name().to_string();
                return Err(BindError::AggError(AggError::ColumnNotInAgg(name)));
            }
        }
        Ok(plan)
    }

    /// Generate an Agg plan for distinct.
    fn plan_distinct(
        &mut self,
        plan: Id,
        distinct: Id,
        orderby: Id,
        projection: &mut Id,
    ) -> Result {
        let distinct_on = self.node(distinct).as_list().to_vec();
        if distinct_on.is_empty() {
            return Ok(plan);
        }
        // make sure all ORDER BY items are in DISTINCT list.
        for id in self.node(orderby).as_list() {
            // id = (asc key) or (desc key)
            let key = self.node(*id).children()[0];
            if !distinct_on.contains(&key) {
                return Err(BindError::AggError(AggError::OrderKeyNotInDistinct));
            }
        }
        // for all projection items that are not in DISTINCT list,
        // wrap them with first() aggregation.
        let mut aggs = vec![];
        let mut projs = self.node(*projection).as_list().to_vec();
        for id in projs.iter_mut() {
            if !distinct_on.contains(id) {
                *id = self.egraph.add(Node::First(*id));
                aggs.push(*id);
            }
        }
        let aggs = self.egraph.add(Node::List(aggs.into()));
        *projection = self.egraph.add(Node::List(projs.into()));
        Ok(self.egraph.add(Node::Agg([aggs, distinct, plan])))
    }
}
