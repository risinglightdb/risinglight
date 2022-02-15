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
    AggKind, BoundAggCall, BoundExpr, BoundInputRef, BoundOrderBy, BoundSelect, BoundTableRef,
};
use crate::optimizer::plan_nodes::{
    Dummy, LogicalAggregate, LogicalFilter, LogicalJoin, LogicalLimit, LogicalOrder,
    LogicalProjection, LogicalTableScan,
};

impl LogicalPlaner {
    pub fn plan_select(&self, mut stmt: Box<BoundSelect>) -> Result<PlanRef, LogicalPlanError> {
        let mut plan: PlanRef = Arc::new(Dummy {});
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
                    stmt.select_list.iter().for_each(|expr| match expr {
                        BoundExpr::AggCall(expr) => {
                            if expr.kind == AggKind::RowCount {
                                with_row_handler = true;
                            }
                        }
                        BoundExpr::ExprWithAlias(expr) => {
                            if let BoundExpr::AggCall(expr) = &*expr.expr {
                                if expr.kind == AggKind::RowCount {
                                    with_row_handler = true;
                                }
                            }
                        }
                        _ => {}
                    });
                }
            }
            plan = self.plan_table_ref(table_ref, with_row_handler, is_sorted)?;
        }

        if let Some(expr) = stmt.where_clause {
            plan = Arc::new(LogicalFilter::new(expr, plan));
        }

        let mut agg_extractor = AggExtractor::new(stmt.group_by.len());
        for expr in &mut stmt.select_list {
            agg_extractor.visit_expr(expr);
        }
        if !agg_extractor.agg_calls.is_empty() {
            plan = Arc::new(LogicalAggregate::new(
                agg_extractor.agg_calls,
                stmt.group_by,
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
                table_name: _,
                column_ids,
                column_descs,
            } => Ok(Arc::new(LogicalTableScan::new(
                *ref_id,
                column_ids.to_vec(),
                column_descs.to_vec(),
                with_row_handler,
                is_sorted,
                None,
            ))),
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
    index: usize,
}

impl AggExtractor {
    fn new(group_key_count: usize) -> Self {
        AggExtractor {
            agg_calls: vec![],
            index: group_key_count,
        }
    }

    fn visit_expr(&mut self, expr: &mut BoundExpr) {
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
                self.visit_expr(&mut bin_op.left_expr);
                self.visit_expr(&mut bin_op.right_expr);
            }
            UnaryOp(unary_op) => self.visit_expr(&mut unary_op.expr),
            TypeCast(type_cast) => self.visit_expr(&mut type_cast.expr),
            ExprWithAlias(expr_with_alias) => self.visit_expr(&mut expr_with_alias.expr),
            IsNull(isnull) => self.visit_expr(&mut isnull.expr),
            Constant(_) | ColumnRef(_) | InputRef(_) | Alias(_) => {}
        }
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
