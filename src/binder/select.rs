// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use sqlparser::tokenizer::Span;

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
                return Err(ErrorKind::Todo("recursive CTE".into()).with_spanned(&with));
            }
            for cte in with.cte_tables {
                self.bind_cte(cte)?;
            }
        }
        let mut child = match *query.body {
            SetExpr::Select(select) => self.bind_select(*select, query.order_by)?,
            SetExpr::Values(values) => self.bind_values(values)?,
            body => return Err(ErrorKind::Todo("unknown set expr".into()).with_spanned(&body)),
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
                return Err(ErrorKind::ColumnCountMismatch(
                    table_alias.clone(),
                    expected_column_num,
                    actual_column_num,
                )
                .with_spanned(&alias));
            }
            alias
                .columns
                .iter()
                .map(|c| Some(c.name.value.to_lowercase()))
                .collect()
        } else {
            // `with t`
            ctx.output_aliases
        };
        self.add_cte(&alias.name, query, column_aliases)?;
        Ok(query)
    }

    fn bind_select(&mut self, select: Select, order_by: Option<OrderBy>) -> Result {
        let from = self.bind_from(select.from)?;

        // bind expressions
        // aggregations, over windows and subqueries will be extracted to the current context
        let projection = self.bind_projection(select.projection, from)?;
        let mut subqueries = self.take_subqueries();
        let where_ = self.bind_where(select.selection)?;
        let groupby = self.bind_groupby(select.group_by)?;
        let mut subqueries_in_agg = self.take_subqueries();
        let having = self.bind_having(select.having)?;
        let orderby = match order_by {
            Some(order_by) => self.bind_orderby(order_by.exprs)?,
            None => self.egraph.add(Node::List([].into())),
        };
        let distinct = match select.distinct {
            None => self.egraph.add(Node::List([].into())),
            Some(Distinct::Distinct) => projection,
            Some(Distinct::On(exprs)) => self.bind_exprs(exprs)?,
        };
        subqueries.extend(self.take_subqueries());
        subqueries_in_agg.extend(subqueries.extract_if(|s| s.in_agg));

        let mut plan = from;
        plan = self.plan_apply(subqueries_in_agg, plan)?;
        plan = self.plan_filter(where_, plan)?;
        let mut to_rewrite = [projection, distinct, having, orderby];
        plan = self.plan_agg(&mut to_rewrite, groupby, plan)?;
        let [mut projection, distinct, having, orderby] = to_rewrite;
        plan = self.plan_apply(subqueries, plan)?;
        plan = self.plan_filter(having, plan)?;
        plan = self.plan_window(plan)?;
        plan = self.plan_distinct(distinct, orderby, &mut projection, plan)?;
        if !self.node(orderby).as_list().is_empty() {
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
                _ => return Err(ErrorKind::Todo("bind select list".into()).with_spanned(&item)),
            }
        }
        self.contexts.last_mut().unwrap().output_aliases = aliases;
        Ok(self.egraph.add(Node::List(select_list.into())))
    }

    /// Binds the WHERE clause. Returns an expression for condition.
    ///
    /// Aggregate functions and window functions are not allowed.
    /// Subqueries will be extracted to the current context.
    pub(super) fn bind_where(&mut self, selection: Option<Expr>) -> Result {
        self.context_mut().in_where = true;
        let id = self.bind_selection(selection)?;
        self.context_mut().in_where = false;
        Ok(id)
    }

    /// Binds the HAVING clause. Returns an expression for condition.
    ///
    /// Window functions are not allowed.
    /// Aggregate functions and subqueries will be extracted to the current context.
    fn bind_having(&mut self, selection: Option<Expr>) -> Result {
        self.context_mut().in_having = true;
        let id = self.bind_selection(selection)?;
        self.context_mut().in_having = false;
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
    /// Aggregate functions and window functions are not allowed.
    fn bind_groupby(&mut self, group_by: GroupByExpr) -> Result<Option<Id>> {
        match group_by {
            GroupByExpr::All(_) => {
                return Err(ErrorKind::Todo("group by all".into()).with_spanned(&group_by))
            }
            GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => return Ok(None),
            GroupByExpr::Expressions(exprs, _) => {
                self.context_mut().in_groupby = true;
                let id = self.bind_exprs(exprs)?;
                self.context_mut().in_groupby = false;
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
                let span = Span::union_iter(row.iter().map(|e| e.span()));
                return Err(ErrorKind::InvalidExpression(
                    "VALUES lists must all be the same length".into(),
                )
                .with_span(span));
            }
            bound_values.push(self.bind_exprs(row)?);
        }
        let id = self.egraph.add(Node::Values(bound_values.into()));
        self.type_(id)?;
        Ok(id)
    }

    pub(super) fn plan_filter(&mut self, predicate: Id, plan: Id) -> Result {
        if self.node(predicate).is_true() {
            return Ok(plan);
        }
        Ok(self.egraph.add(Node::Filter([predicate, plan])))
    }

    /// Extracts all aggregations from `exprs` and generates an [`Agg`](Node::Agg) plan.
    /// If no aggregation is found and no `groupby` keys, returns the original `plan`.
    fn plan_agg(&mut self, exprs: &mut [Id], groupby: Option<Id>, plan: Id) -> Result {
        let mut aggs = self.context().aggregates.clone();
        if aggs.is_empty() && groupby.is_none() {
            return Ok(plan);
        }
        // make sure the order of the aggs is deterministic
        aggs.sort();
        aggs.dedup();
        let aggs = self.egraph.add(Node::List(aggs.into()));
        let plan = self.egraph.add(match groupby {
            Some(groupby) => Node::HashAgg([groupby, aggs, plan]),
            None => Node::Agg([aggs, plan]),
        });
        // check for not aggregated columns
        // rewrite the expressions with a wrapper over group keys
        if let Some(groupby) = groupby {
            let groupby = self.schema(groupby);
            for id in exprs {
                *id = self.rewrite_agg_in_expr(*id, &groupby)?;
            }
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
            return Ok(self.wrap_ref(id));
        }
        if let Node::Ref(_) = &expr {
            return Ok(id);
        }
        if let Node::Column(cid) = &expr {
            let name = self.catalog.get_column(cid).unwrap().name().to_string();
            return Err(ErrorKind::ColumnNotInAgg(name).into());
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
                return Err(ErrorKind::OrderKeyNotInDistinct.into());
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
        let mut overs = self.contexts.last().unwrap().over_windows.clone();
        if overs.is_empty() {
            return Ok(plan);
        }
        overs.sort();
        overs.dedup();
        let overs = self.egraph.add(Node::List(overs.into()));
        Ok(self.egraph.add(Node::Window([overs, plan])))
    }

    /// Generate an [`Apply`](Node::Apply) plan for each subquery in the current context.
    pub(super) fn plan_apply(&mut self, subqueries: Vec<Subquery>, mut plan: Id) -> Result {
        for subquery in subqueries {
            let type_id = self.egraph.add(if subquery.exists {
                Node::Mark
            } else {
                Node::LeftOuter
            });
            plan = self
                .egraph
                .add(Node::Apply([type_id, plan, subquery.query_id]));
        }
        Ok(plan)
    }
}
