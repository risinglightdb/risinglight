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
    BoundAggCall, BoundExpr, BoundInputRef, BoundOrderBy, BoundSelect, BoundTableRef, ExprVisitor,
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
            if let BoundTableRef::JoinTableRef {
                relation,
                join_tables,
            } = table_ref
            {
                if let BoundTableRef::BaseTableRef { column_ids, .. } = &**relation {
                    if join_tables.is_empty() && column_ids.is_empty() {
                        stmt.select_list.iter().for_each(|expr| {
                            if expr.contains_row_count() && !expr.contains_column_ref() {
                                with_row_handler = true;
                            }
                        });
                    }
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

        let mut agg_extractor = AggExtractor::new();
        for expr in &mut stmt.select_list {
            agg_extractor.visit_select_expr(expr);
        }
        for expr in &mut stmt.group_by {
            agg_extractor.visit_group_by_expr(expr, &stmt.select_list);
        }
        if !stmt.group_by.is_empty() {
            agg_extractor.validate_illegal_column(&stmt.select_list)?;
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

/// An expression visitor that extracts aggregation nodes and validate illegal select exprs.
/// Visotor will also rewrite group by alias expression to corresponding select expression and then
/// validate illegal select exprs.
#[derive(Default)]
struct AggExtractor {
    agg_calls: Vec<BoundAggCall>,
    group_by_exprs: Vec<BoundExpr>,
}

impl AggExtractor {
    fn new() -> Self {
        AggExtractor {
            agg_calls: vec![],
            group_by_exprs: vec![],
        }
    }

    /// Validate select exprs must appear in the GROUP BY clause or be used in an aggregate
    /// function. Need `visit_group_by_expr` to rewrite the group by alias first.
    /// TODO: add order by exprs validation
    fn validate_illegal_column(
        &mut self,
        select_exprs: &[BoundExpr],
    ) -> Result<(), LogicalPlanError> {
        use BoundExpr::ExprWithAlias;

        for mut expr in select_exprs {
            if let ExprWithAlias(e) = expr {
                expr = &*e.expr;
            }

            if expr.contains_agg_call() {
                continue;
            }
            if !self.group_by_exprs.iter().contains(expr) {
                return Err(LogicalPlanError::IllegalGroupBySQL(format!(r#"{}"#, expr)));
            }
        }
        Ok(())
    }

    fn visit_select_expr(&mut self, expr: &mut BoundExpr) {
        struct Visitor<'a>(&'a mut Vec<BoundAggCall>);
        impl<'a> ExprVisitor for Visitor<'a> {
            fn visit_agg_call(&mut self, agg: &BoundAggCall) {
                self.0.push(agg.clone());
            }
        }
        let mut agg_calls = vec![];
        Visitor(&mut agg_calls).visit_expr(expr);
        // TODO: handle duplicate agg_call
        self.agg_calls.extend_from_slice(&agg_calls);
    }

    fn visit_group_by_expr(&mut self, expr: &mut BoundExpr, select_list: &[BoundExpr]) {
        use BoundExpr::*;
        if let Alias(alias) = expr {
            // rewrite group by alias to corresponding select expr
            if let Some(expr) = select_list.iter().find_map(|inner_expr| {
                if let ExprWithAlias(e) = inner_expr {
                    if e.alias == alias.alias {
                        return Some(*e.expr.clone());
                    }
                }
                None
            }) {
                self.group_by_exprs.push(expr);
                return;
            }
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

#[cfg(test)]
mod tests {
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::{BoundAlias, BoundBinaryOp, BoundColumnRef, BoundExprWithAlias};
    use crate::catalog::ColumnRefId;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};

    #[test]
    fn test_agg_extractor_validate_illegal_column() {
        // case1 sql: select v2 + 1 from t group by v2 + 1
        let v2_plus_1 = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: build_column_ref(1, "v2".to_string()).into(),
            right_expr: BoundExpr::Constant(DataValue::Int32(1)).into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        assert!(
            validate_illegal_column(&mut [v2_plus_1.clone()], &mut [v2_plus_1.clone()]).is_ok()
        );

        // case2 sql: select v2 + 1, v1 from t group by v2 + 1
        let v1 = build_column_ref(0, "v1".to_string());
        assert!(validate_illegal_column(
            &mut [v2_plus_1.clone(), v1.clone()],
            &mut [v2_plus_1.clone()]
        )
        .is_err());

        // case3 sql: select v2 + 1 as a, v1 as b from t group by a
        let v2_plus_1_alias_a = BoundExpr::ExprWithAlias(BoundExprWithAlias {
            expr: v2_plus_1.into(),
            alias: "a".to_string(),
        });
        let v1_alias_b = BoundExpr::ExprWithAlias(BoundExprWithAlias {
            expr: v1.into(),
            alias: "b".to_string(),
        });
        let alias_a = BoundExpr::Alias(BoundAlias {
            alias: "a".to_string(),
        });
        assert!(
            validate_illegal_column(&mut [v2_plus_1_alias_a, v1_alias_b], &mut [alias_a]).is_err()
        );
    }

    fn build_column_ref(column_id: u32, column_name: String) -> BoundExpr {
        BoundExpr::ColumnRef(BoundColumnRef {
            column_ref_id: ColumnRefId::new(0, 0, 0, column_id),
            is_primary_key: false,
            desc: DataTypeKind::Int(None).not_null().to_column(column_name),
        })
    }

    fn validate_illegal_column(
        select_list: &mut [BoundExpr],
        group_by_list: &mut [BoundExpr],
    ) -> Result<(), LogicalPlanError> {
        let mut extractor = AggExtractor::new();
        for expr in group_by_list {
            extractor.visit_group_by_expr(expr, select_list);
        }
        extractor.validate_illegal_column(select_list)?;
        Ok(())
    }
}
