// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::parser::{Expr, Query, SelectItem, SetExpr};

impl Binder {
    /// Binds a query in a new sub-context.
    pub(super) fn bind_query(&mut self, query: Query) -> Result<(Id, Context)> {
        let context = Context {
            // inherit all variable ids from the parent context
            all_variable_ids: self.context().all_variable_ids.clone(),
            ..Default::default()
        };
        self.contexts.push(context);
        let ret = self.bind_query_internal(query);
        let ctx = self.contexts.pop().unwrap();
        ret.map(|id| (id, ctx))
    }

    /// Binds a query in the current context.
    pub(super) fn bind_query_internal(&mut self, query: Query) -> Result {
        if let Some(with) = query.with {
            if with.recursive {
                return Err(BindError::Todo("recursive CTE".into()));
            }
            for cte in with.cte_tables {
                self.bind_cte(cte)?;
            }
        }
        let mut child = match *query.body {
            SetExpr::Select(select) => self.bind_select(*select, query.order_by)?,
            SetExpr::Values(values) => self.bind_values(values)?,
            _ => return Err(BindError::Todo("unknown set expr".into())),
        };
        if query.limit.is_some() || query.offset.is_some() {
            let limit = match query.limit {
                Some(expr) => self.bind_expr(expr)?,
                None => self.egraph.add(Node::null()),
            };
            let offset = match query.offset {
                Some(offset) => self.bind_expr(offset.value)?,
                None => self.egraph.add(Node::zero()),
            };
            child = self.egraph.add(Node::Limit([limit, offset, child]));
        }
        Ok(child)
    }

    /// Binds a CTE definition: `alias AS query`.
    ///
    /// Returns a node of query and adds the CTE to the context.
    fn bind_cte(&mut self, Cte { alias, query, .. }: Cte) -> Result {
        let table_alias = alias.name.value.to_lowercase();
        let (query, ctx) = self.bind_query(*query)?;
        let column_aliases = if !alias.columns.is_empty() {
            // `with t(a, b, ..)`
            // check column count
            let expected_column_num = self.schema(query).len();
            let actual_column_num = alias.columns.len();
            if actual_column_num != expected_column_num {
                return Err(BindError::ColumnCountMismatch(
                    table_alias.clone(),
                    expected_column_num,
                    actual_column_num,
                ));
            }
            alias
                .columns
                .iter()
                .map(|c| Some(c.value.to_lowercase()))
                .collect()
        } else {
            // `with t`
            ctx.output_aliases
        };
        self.add_cte(&table_alias, query, column_aliases)?;
        Ok(query)
    }

    fn bind_select(&mut self, select: Select, order_by: Vec<OrderByExpr>) -> Result {
        let from = self.bind_from(select.from)?;

        // bind expressions
        // aggregations, over windows and subqueries will be extracted to the current context
        let mut projection = self.bind_projection(select.projection, from)?;
        let where_ = self.bind_where(select.selection)?;
        let groupby = self.bind_groupby(select.group_by)?;
        let having = self.bind_having(select.having)?;
        let orderby = self.bind_orderby(order_by)?;
        let distinct = match select.distinct {
            None => self.egraph.add(Node::List([].into())),
            Some(Distinct::Distinct) => projection,
            Some(Distinct::On(exprs)) => self.bind_exprs(exprs)?,
        };

        let mut plan = from;
        plan = self.plan_apply(plan)?;
        if self.node(where_) != &Node::true_() {
            plan = self.egraph.add(Node::Filter([where_, plan]));
        }
        plan = self.plan_agg(groupby, plan)?;
        if self.node(having) != &Node::true_() {
            plan = self.egraph.add(Node::Filter([having, plan]));
        }
        plan = self.plan_window(plan)?;
        plan = self.plan_distinct(distinct, orderby, &mut projection, plan)?;
        if self.node(orderby) != &Node::List([].into()) {
            plan = self.egraph.add(Node::Order([orderby, plan]));
        }
        plan = self.egraph.add(Node::Proj([projection, plan]));
        Ok(plan)
    }

    /// Binds the select list. Returns a list of expressions.
    fn bind_projection(&mut self, projection: Vec<SelectItem>, from: Id) -> Result {
        let mut select_list = vec![];
        let mut aliases = vec![];
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let ident = if let Expr::Identifier(ident) = &expr {
                        Some(ident.value.to_lowercase())
                    } else {
                        None
                    };
                    let id = self.bind_expr(expr)?;
                    aliases.push(ident);
                    select_list.push(id);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let id = self.bind_expr(expr)?;
                    let name = alias.value.to_lowercase();
                    self.add_alias(name.clone(), "".into(), id);
                    aliases.push(Some(name));
                    select_list.push(id);
                }
                SelectItem::Wildcard(_) => {
                    for id in self.schema(from) {
                        let id = self.wrap_ref(id);
                        select_list.push(id);
                    }
                    aliases.resize(select_list.len(), None);
                }
                _ => return Err(BindError::Todo("bind select list".into())),
            }
        }
        self.contexts.last_mut().unwrap().output_aliases = aliases;
        Ok(self.egraph.add(Node::List(select_list.into())))
    }

    /// Binds the WHERE clause. Returns an expression for condition.
    ///
    /// Raises an error if there is an aggregation or over window in the expression.
    pub(super) fn bind_where(&mut self, selection: Option<Expr>) -> Result {
        let num_aggs = self.num_aggregations();
        let num_overs = self.num_over_windows();

        let id = self.bind_selection(selection)?;

        if self.num_aggregations() > num_aggs {
            return Err(BindError::AggInWhere);
        }
        if self.num_over_windows() > num_overs {
            return Err(BindError::WindowInWhere);
        }
        Ok(id)
    }

    /// Binds the HAVING clause. Returns an expression for condition.
    ///
    /// Raises an error if there is an over window in the expression.
    fn bind_having(&mut self, selection: Option<Expr>) -> Result {
        let num_overs = self.num_over_windows();

        let id = self.bind_selection(selection)?;

        if self.num_over_windows() > num_overs {
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
    /// Raises an error if there is an aggregation in the expressions.
    fn bind_groupby(&mut self, group_by: GroupByExpr) -> Result<Option<Id>> {
        match group_by {
            GroupByExpr::All => return Err(BindError::Todo("group by all".into())),
            GroupByExpr::Expressions(group_by) if group_by.is_empty() => return Ok(None),
            GroupByExpr::Expressions(group_by) => {
                let num_aggs = self.num_aggregations();
                let id = self.bind_exprs(group_by)?;
                if self.num_aggregations() > num_aggs {
                    return Err(BindError::AggInGroupBy);
                }
                Ok(Some(id))
            }
        }
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
        self.type_(id)?;
        Ok(id)
    }

    /// Extracts all aggregations from `exprs` and generates an [`Agg`](Node::Agg) plan.
    /// If no aggregation is found and no `groupby` keys, returns the original `plan`.
    fn plan_agg(&mut self, groupby: Option<Id>, plan: Id) -> Result {
        let aggs = &self.contexts.last().unwrap().aggregates;
        if aggs.is_empty() && groupby.is_none() {
            return Ok(plan);
        }
        // make sure the order of the aggs is deterministic
        let mut aggs = aggs.iter().cloned().collect_vec();
        aggs.sort();
        let aggs = self.egraph.add(Node::List(aggs.into()));
        let plan = self.egraph.add(match groupby {
            Some(groupby) => Node::HashAgg([groupby, aggs, plan]),
            None => Node::Agg([aggs, plan]),
        });
        // check for not aggregated columns
        // rewrite the expressions with a wrapper over agg or group keys
        // let schema = self.schema(plan);
        // for id in exprs {
        //     *id = self.rewrite_agg_in_expr(*id, &schema)?;
        // }
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
        // stop at subquery
        // XXX: maybe wrong
        if let Node::Max1Row(_) = &expr {
            return Ok(id);
        }
        if schema.contains(&id) {
            return Ok(self.wrap_ref(id));
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
    /// ```text
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
            if !distinct_on.contains(key) {
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
        Ok(self.egraph.add(Node::HashAgg([distinct, aggs, plan])))
    }

    /// Generates an [`Window`](Plan::Window) plan if any over node is found in the current context.
    /// Otherwise returns the original `plan`.
    fn plan_window(&mut self, plan: Id) -> Result {
        let overs = &self.contexts.last().unwrap().over_windows;
        if overs.is_empty() {
            return Ok(plan);
        }
        let mut overs = overs.iter().cloned().collect_vec();
        overs.sort();
        let overs = self.egraph.add(Node::List(overs.into()));
        Ok(self.egraph.add(Node::Window([overs, plan])))
    }

    /// Generate an [`Apply`](Node::Apply) plan for each subquery in the current context.
    fn plan_apply(&mut self, mut plan: Id) -> Result {
        let left_outer = self.egraph.add(Node::LeftOuter);
        for subquery in self.context().subqueries.clone() {
            plan = self.egraph.add(Node::Apply([left_outer, plan, subquery]));
        }
        let mark = self.egraph.add(Node::Mark);
        for subquery in self.context().exists_subqueries.clone() {
            plan = self.egraph.add(Node::Apply([mark, plan, subquery]));
        }
        Ok(plan)
    }
}
