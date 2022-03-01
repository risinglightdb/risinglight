// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Logical planner of `select` statement.
//!
//! A `select` statement will be planned to a compose of:
//!
//! - [`LogicalTableScan`] (from *) or dummy plan (no from)
//! - [`LogicalFilter`] (where *)
//! - [`LogicalProjection`] (select *)
//! - [`LogicalOrder`] (order by *)
use itertools::Itertools;

use super::*;
use crate::binder::{
    BoundAggCall, BoundExpr, BoundInputRef, BoundOrderBy, BoundSelect, BoundTableRef,
};
use crate::optimizer::plan_nodes::{
    Internal, LogicalAggregate, LogicalFilter, LogicalJoin, LogicalLimit, LogicalOrder,
    LogicalProjection, LogicalTableScan, LogicalValues,
};

impl LogicalPlaner {
    pub fn plan_select(&self, mut stmt: Box<BoundSelect>) -> Result<PlanRef, LogicalPlanError> {
        let mut plan: PlanRef;
        let mut is_sorted = false;
        let mut with_row_handler = false;

        if let Some(table_ref) = &stmt.from_table {
            // use `sorted` mode from the storage engine if the order by column is the primary key
            if stmt.orderby.len() == 1 && !stmt.orderby[0].descending {
                if let BoundExpr::ColumnRef(col_ref) = &stmt.orderby[0].expr {
                    if col_ref.is_primary_key {
                        is_sorted = true;
                    }
                }
            }
            if let BoundTableRef::JoinTableRef { join_tables, .. } = table_ref {
                if join_tables.is_empty() {
                    stmt.select_list.iter().for_each(|expr| {
                        if expr.contains_row_count() && !expr.contains_column_ref() {
                            with_row_handler = true;
                        }
                    });
                }
            }
            plan = self.plan_table_ref(table_ref, with_row_handler, is_sorted)?;
        } else {
            plan = Arc::new(LogicalValues::new(
                stmt.select_list
                    .iter()
                    .map(|expr| expr.return_type().unwrap())
                    .collect_vec(),
                vec![stmt.select_list.clone()],
            ));
            return Ok(plan);
        }

        if let Some(expr) = stmt.where_clause {
            plan = Arc::new(LogicalFilter::new(expr, plan));
        }

        let mut agg_extractor = AggExtractor::new(stmt.group_by.len());

        if !stmt.group_by.is_empty() {
            agg_extractor.validate_illegal_column(&stmt.select_list, &stmt.group_by)?;
        }
        for expr in &mut stmt.select_list {
            agg_extractor.visit_select_expr(expr);
        }
        for expr in &mut stmt.group_by {
            agg_extractor.visit_group_by_expr(expr, &mut stmt.select_list);
        }
        if !agg_extractor.agg_calls.is_empty() || !agg_extractor.group_by_exprs.is_empty() {
            plan = Arc::new(LogicalAggregate::new(
                agg_extractor.agg_calls,
                agg_extractor.group_by_exprs,
                plan,
            ));
        }

        let mut alias_extractor = AliasExtractor::new(&stmt.select_list);
        let comparators = stmt
            .orderby
            .into_iter()
            .map(|expr| alias_extractor.visit_expr(expr))
            .collect_vec();

        // TODO: support the following clauses
        assert!(!stmt.select_distinct, "TODO: plan distinct");

        if !stmt.select_list.is_empty() {
            plan = Arc::new(LogicalProjection::new(stmt.select_list, plan));
        }
        if !comparators.is_empty() && !is_sorted {
            plan = Arc::new(LogicalOrder::new(comparators, plan));
        }
        if stmt.limit.is_some() || stmt.offset.is_some() {
            let limit = match stmt.limit {
                Some(limit) => match limit {
                    BoundExpr::Constant(v) => v.as_usize()?.unwrap_or(usize::MAX / 2),
                    _ => panic!("limit only support constant expression"),
                },
                None => usize::MAX / 2, // avoid 'offset + limit' overflow
            };
            let offset = match stmt.offset {
                Some(offset) => match offset {
                    BoundExpr::Constant(v) => v.as_usize()?.unwrap_or(0),
                    _ => panic!("offset only support constant expression"),
                },
                None => 0,
            };
            plan = Arc::new(LogicalLimit::new(offset, limit, plan));
        }
        Ok(plan)
    }

    pub fn plan_table_ref(
        &self,
        table_ref: &BoundTableRef,
        with_row_handler: bool,
        is_sorted: bool,
    ) -> Result<PlanRef, LogicalPlanError> {
        match table_ref {
            BoundTableRef::BaseTableRef {
                ref_id,
                table_name,
                column_ids,
                column_descs,
                is_internal,
            } => {
                if *is_internal {
                    Ok(Arc::new(Internal::new(
                        table_name.clone(),
                        *ref_id,
                        column_ids.to_vec(),
                        column_descs.to_vec(),
                    )))
                } else {
                    Ok(Arc::new(LogicalTableScan::new(
                        *ref_id,
                        column_ids.to_vec(),
                        column_descs.to_vec(),
                        with_row_handler,
                        is_sorted,
                        None,
                    )))
                }
            }
            BoundTableRef::JoinTableRef {
                relation,
                join_tables,
            } => {
                let mut plan = self.plan_table_ref(relation, with_row_handler, is_sorted)?;
                for join_table in join_tables.iter() {
                    let table_plan =
                        self.plan_table_ref(&join_table.table_ref, with_row_handler, is_sorted)?;
                    plan = Arc::new(LogicalJoin::create(
                        plan,
                        table_plan,
                        join_table.join_op,
                        join_table.join_cond.clone(),
                    ));
                }
                Ok(plan)
            }
        }
    }
}

/// An expression visitor that extracts aggregation nodes and replaces them with `InputRef`.
///
/// For example:
/// In SQL: `select sum(b) + a * count(a) from t group by a;`
/// The expression `sum(b) + a * count(a)` will be rewritten to `InputRef(1) + a * InputRef(2)`,
/// because the underlying aggregate plan will output `(a, sum(b), count(a))`. The group keys appear
/// before aggregations.
#[derive(Default)]
struct AggExtractor {
    agg_calls: Vec<BoundAggCall>,
    group_by_exprs: Vec<BoundExpr>,
    index: usize,
}

impl AggExtractor {
    fn new(group_key_count: usize) -> Self {
        AggExtractor {
            agg_calls: vec![],
            group_by_exprs: vec![],
            index: group_key_count,
        }
    }

    /// validate select exprs must appear in the GROUP BY clause or be used in an aggregate function
    fn validate_illegal_column(
        &mut self,
        select_exprs: &[BoundExpr],
        group_by_exprs: &[BoundExpr],
    ) -> Result<(), LogicalPlanError> {
        use BoundExpr::*;
        let mut group_by_raw_exprs = vec![];
        group_by_exprs.iter().for_each(|e| {
            if let Alias(alias) = e {
                let alias_expr = select_exprs.iter().find(|inner_expr| {
                    if let ExprWithAlias(e) = inner_expr {
                        e.alias == alias.alias
                    } else {
                        false
                    }
                });
                if let Some(inner_expr) = alias_expr {
                    group_by_raw_exprs.push(inner_expr.clone());
                }
            } else {
                group_by_raw_exprs.push(e.clone());
            }
        });

        for expr in select_exprs {
            if expr.contains_agg_call() {
                continue;
            }
            if !group_by_raw_exprs.iter().contains(expr) {
                return Err(LogicalPlanError::IllegalGroupBySQL(format!(r#"{}"#, expr)));
            }
        }
        Ok(())
    }

    fn visit_select_expr(&mut self, expr: &mut BoundExpr) {
        use BoundExpr::*;
        match expr {
            AggCall(agg) => {
                let input_ref = InputRef(BoundInputRef {
                    index: self.index,
                    return_type: agg.return_type.clone(),
                });
                match std::mem::replace(expr, input_ref) {
                    AggCall(agg) => self.agg_calls.push(agg),
                    _ => unreachable!(),
                }
                self.index += 1;
            }
            BinaryOp(bin_op) => {
                self.visit_select_expr(&mut bin_op.left_expr);
                self.visit_select_expr(&mut bin_op.right_expr);
            }
            UnaryOp(unary_op) => self.visit_select_expr(&mut unary_op.expr),
            TypeCast(type_cast) => self.visit_select_expr(&mut type_cast.expr),
            ExprWithAlias(expr_with_alias) => self.visit_select_expr(&mut expr_with_alias.expr),
            IsNull(isnull) => self.visit_select_expr(&mut isnull.expr),
            Constant(_) | ColumnRef(_) | InputRef(_) | Alias(_) => {}
        }
    }

    fn visit_group_by_expr(&mut self, expr: &mut BoundExpr, select_list: &mut Vec<BoundExpr>) {
        use BoundExpr::*;
        if let Alias(alias) = expr {
            if let Some(i) = select_list.iter().position(|inner_expr| {
                if let ExprWithAlias(e) = inner_expr {
                    e.alias == alias.alias
                } else {
                    false
                }
            }) {
                let select_item = &mut select_list[i];
                self.group_by_exprs.push(std::mem::replace(
                    select_item,
                    InputRef(BoundInputRef {
                        index: self.group_by_exprs.len(),
                        return_type: select_item.return_type().unwrap(),
                    }),
                ));
                return;
            }
        }

        if let Some(i) = select_list.iter().position(|e| e == expr) {
            match select_list[i] {
                Constant(_) | ColumnRef(_) => self.group_by_exprs.push(select_list[i].clone()),
                _ => {
                    self.group_by_exprs.push(std::mem::replace(
                        &mut select_list[i],
                        InputRef(BoundInputRef {
                            index: self.group_by_exprs.len(),
                            return_type: expr.return_type().unwrap(),
                        }),
                    ));
                }
            }
            return;
        }

        self.group_by_exprs.push(expr.clone());
    }
}

/// And expression visitor that extracts aliases in order-by expressions and replaces them with
/// `InputRef`.
///
/// For example,
/// In SQL: `select a, b as c from t order by c;`
/// The expression `c` in the order-by clause will be rewritten to `InputRef(1)`, because the
/// underlying projection plan will output `(a, b)`, where `b` is alias to `c`.
#[derive(Default)]
struct AliasExtractor<'a> {
    select_list: &'a [BoundExpr],
}

impl<'a> AliasExtractor<'a> {
    fn new(select_list: &'a [BoundExpr]) -> Self {
        AliasExtractor { select_list }
    }

    fn visit_expr(&mut self, expr: BoundOrderBy) -> BoundOrderBy {
        use BoundExpr::{Alias, ColumnRef, ExprWithAlias, InputRef};
        match expr.expr {
            Alias(alias) => {
                // Binder has pushed the alias expression to `select_list`, so we can unwrap
                // directly
                let index = self
                    .select_list
                    .iter()
                    .position(|inner_expr| {
                        if let ExprWithAlias(e) = inner_expr {
                            e.alias == alias.alias
                        } else {
                            false
                        }
                    })
                    .unwrap();
                let select_item = &self.select_list[index];
                let input_ref = InputRef(BoundInputRef {
                    index,
                    return_type: select_item.return_type().unwrap(),
                });
                BoundOrderBy {
                    expr: input_ref,
                    descending: expr.descending,
                }
            }
            ColumnRef(_) => expr,
            _ => panic!("order-by expression should be column ref or expr alias"),
        }
    }
}
