// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::parser::{Expr, Query, SelectItem, SetExpr};

impl Binder {
    pub(super) fn bind_query(&mut self, query: Query) -> Result<(Id, Context)> {
        self.contexts.push(Context::default());
        let ret = self.bind_query_internal(query);
        let ctx = self.contexts.pop().unwrap();
        ret.map(|id| (id, ctx))
    }

    pub(super) fn bind_query_internal(&mut self, query: Query) -> Result {
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
        let projection = self.bind_projection(select.projection, from)?;
        let where_ = self.bind_where(select.selection)?;
        let groupby = match select.group_by {
            group_by if group_by.is_empty() => None,
            group_by => Some(self.bind_groupby(group_by)?),
        };
        let having = self.bind_having(select.having)?;
        let orderby = self.bind_orderby(order_by)?;
        let distinct = match select.distinct {
            // TODO: distinct on
            true => projection,
            false => self.egraph.add(Node::List([].into())),
        };

        let mut plan = self.egraph.add(Node::Filter([where_, from]));
        let mut to_rewrite = [projection, distinct, having, orderby];
        plan = self.plan_agg(&mut to_rewrite, groupby, plan)?;
        let [mut projection, distinct, having, orderby] = to_rewrite;
        plan = self.egraph.add(Node::Filter([having, plan]));
        plan = self.plan_window(projection, distinct, orderby, plan)?;
        plan = self.plan_distinct(distinct, orderby, &mut projection, plan)?;
        plan = self.egraph.add(Node::Order([orderby, plan]));
        plan = self.egraph.add(Node::Proj([projection, plan]));
        Ok(plan)
    }

    /// Binds the select list. Returns a list of expressions.
    fn bind_projection(&mut self, projection: Vec<SelectItem>, from: Id) -> Result {
        let mut select_list = vec![];
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let ident = if let Expr::Identifier(ident) = &expr {
                        Some(ident.value.to_lowercase())
                    } else {
                        None
                    };
                    let id = self.bind_expr(expr)?;
                    if let Some(ident) = ident {
                        self.current_ctx_mut().output_aliases.insert(ident, id);
                    }
                    select_list.push(id);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let id = self.bind_expr(expr)?;
                    let ref_id = self.egraph.add(Node::Ref(id));
                    self.add_alias(alias.value.to_lowercase(), "".into(), id);
                    self.current_ctx_mut()
                        .output_aliases
                        .insert(alias.value, ref_id);
                    select_list.push(id);
                }
                SelectItem::Wildcard(_) => {
                    select_list.append(&mut self.schema(from));
                }
                _ => todo!("bind select list"),
            }
        }
        Ok(self.egraph.add(Node::List(select_list.into())))
    }

    /// Binds the WHERE clause. Returns an expression for condition.
    ///
    /// There should be no aggregation in the expression, otherwise an error will be returned.
    pub(super) fn bind_where(&mut self, selection: Option<Expr>) -> Result {
        let id = self.bind_selection(selection)?;
        if !self.aggs(id).is_empty() {
            return Err(BindError::AggInWhere);
        }
        if !self.overs(id).is_empty() {
            return Err(BindError::WindowInWhere);
        }
        Ok(id)
    }

    /// Binds the HAVING clause. Returns an expression for condition.
    fn bind_having(&mut self, selection: Option<Expr>) -> Result {
        let id = self.bind_selection(selection)?;
        if !self.overs(id).is_empty() {
            return Err(BindError::WindowInHaving);
        }
        Ok(id)
    }

    /// Binds a selection. Returns a `true` node if no selection.
    fn bind_selection(&mut self, selection: Option<Expr>) -> Result {
        Ok(match selection {
            Some(expr) => self.bind_expr(expr)?,
            None => self.egraph.add(Node::true_()),
        })
    }

    /// Binds the GROUP BY clause. Returns a list of expressions.
    ///
    /// There should be no aggregation in the expressions, otherwise an error will be returned.
    fn bind_groupby(&mut self, group_by: Vec<Expr>) -> Result {
        let id = self.bind_exprs(group_by)?;
        if !self.aggs(id).is_empty() {
            return Err(BindError::AggInGroupBy);
        }
        Ok(id)
    }

    /// Binds the ORDER BY clause. Returns a list of expressions.
    pub(super) fn bind_orderby(&mut self, order_by: Vec<OrderByExpr>) -> Result {
        let mut orderby = Vec::with_capacity(order_by.len());
        for e in order_by {
            let expr = self.bind_expr(e.expr)?;
            let key = match e.asc {
                Some(true) | None => expr,
                Some(false) => self.egraph.add(Node::Desc(expr)),
            };
            orderby.push(key);
        }
        Ok(self.egraph.add(Node::List(orderby.into())))
    }

    /// Binds the VALUES clause. Returns a [`Values`](Node::Values) plan.
    fn bind_values(&mut self, values: Values) -> Result {
        let values = values.rows;
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
            bound_values.push(self.bind_exprs(row)?);
        }
        let id = self.egraph.add(Node::Values(bound_values.into()));
        self.check_type(id)?;
        Ok(id)
    }

    /// Extracts all aggregations from `exprs` and generates an [`Agg`](Node::Agg) plan.
    /// If no aggregation is found and no `groupby` keys, returns the original `plan`.
    fn plan_agg(&mut self, exprs: &mut [Id], groupby: Option<Id>, plan: Id) -> Result {
        let expr_list = self.egraph.add(Node::List(exprs.to_vec().into()));
        let aggs = self.aggs(expr_list).to_vec();
        if aggs.is_empty() && groupby.is_none() {
            return Ok(plan);
        }
        // check nested agg
        for child in aggs.iter().flat_map(|agg| agg.children()) {
            if !self.aggs(*child).is_empty() {
                return Err(BindError::NestedAgg);
            }
        }
        let mut list: Vec<_> = aggs.into_iter().map(|agg| self.egraph.add(agg)).collect();
        // make sure the order of the aggs is deterministic
        list.sort();
        list.dedup();
        let aggs = self.egraph.add(Node::List(list.into()));
        let plan = self.egraph.add(match groupby {
            Some(groupby) => Node::HashAgg([aggs, groupby, plan]),
            None => Node::Agg([aggs, plan]),
        });
        // check for not aggregated columns
        // rewrite the expressions with a wrapper over agg or group keys
        let schema = self.schema(plan);
        for id in exprs {
            *id = self.rewrite_agg_in_expr(*id, &schema)?;
        }
        Ok(plan)
    }

    /// Rewrites the expression `id` with aggs wrapped in a [`Ref`](Node::Ref) node.
    /// Returns the new expression.
    ///
    /// # Example
    /// ```text
    /// id:         (+ (sum a) (+ b 1))
    /// schema:     (sum a), (+ b 1)
    /// output:     (+ (ref (sum a)) (ref (+ b 1)))
    ///
    /// so that `id` won't be optimized to:
    ///             (+ b (+ (sum a) 1))
    /// which can not be composed by `schema`
    /// ```
    fn rewrite_agg_in_expr(&mut self, id: Id, schema: &[Id]) -> Result {
        let mut expr = self.node(id).clone();
        if schema.contains(&id) {
            // found agg, wrap it with Ref
            return Ok(self.egraph.add(Node::Ref(id)));
        }
        if let Node::Column(cid) = &expr {
            let name = self.catalog.get_column(cid).unwrap().name().to_string();
            return Err(BindError::ColumnNotInAgg(name));
        }
        for child in expr.children_mut() {
            *child = self.rewrite_agg_in_expr(*child, schema)?;
        }
        Ok(self.egraph.add(expr))
    }

    /// Generate an [`Agg`](Node::Agg) plan for DISTINCT.
    ///
    /// The `distinct` list will become the group by keys of the new aggregation.
    /// All items in `projection` that are not in `distinct` list
    /// will be wrapped with a [`first`](Node::First) aggregation.
    ///
    /// If `distinct` is an empty list, returns the original `plan`.
    ///
    /// # Example
    /// ```ignore
    /// distinct=(list a b)
    /// projection=(list b c)
    /// output=(hashagg (list b (first c)) (list a b) plan)
    /// ```
    fn plan_distinct(
        &mut self,
        distinct: Id,
        orderby: Id,
        projection: &mut Id,
        plan: Id,
    ) -> Result {
        let distinct_on = self.node(distinct).as_list().to_vec();
        if distinct_on.is_empty() {
            return Ok(plan);
        }
        // make sure all ORDER BY items are in DISTINCT list.
        for id in self.node(orderby).as_list() {
            // id = key or (desc key)
            let key = match self.node(*id) {
                Node::Desc(id) => id,
                _ => id,
            };
            if !distinct_on.contains(&key) {
                return Err(BindError::OrderKeyNotInDistinct);
            }
        }
        // for all projection items that are not in DISTINCT list,
        // wrap them with first() aggregation.
        let mut aggs = vec![];
        let mut projs = self.node(*projection).as_list().to_vec();
        for id in &mut projs {
            if !distinct_on.contains(id) {
                *id = self.egraph.add(Node::First(*id));
                aggs.push(*id);
            }
        }
        let aggs = self.egraph.add(Node::List(aggs.into()));
        *projection = self.egraph.add(Node::List(projs.into()));
        Ok(self.egraph.add(Node::HashAgg([aggs, distinct, plan])))
    }

    /// Extracts all over nodes from `projection`, `distinct` and `orderby`.
    /// Generates an [`Window`](Node::Window) plan if any over node is found.
    /// Otherwise returns the original `plan`.
    fn plan_window(&mut self, projection: Id, distinct: Id, orderby: Id, plan: Id) -> Result {
        let mut overs = vec![];
        overs.extend_from_slice(self.overs(projection));
        overs.extend_from_slice(self.overs(distinct));
        overs.extend_from_slice(self.overs(orderby));

        if overs.is_empty() {
            return Ok(plan);
        }
        let mut list: Vec<_> = overs
            .into_iter()
            .map(|over| self.egraph.add(over))
            .collect();
        list.sort();
        list.dedup();
        let overs = self.egraph.add(Node::List(list.into()));
        Ok(self.egraph.add(Node::Window([overs, plan])))
    }
}
