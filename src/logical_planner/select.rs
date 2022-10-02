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
use crate::optimizer::logical_plan_rewriter::ExprRewriter;
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
            // Support scalar having: SELECT 42 HAVING 42 > 18;
            if let Some(having) = &stmt.having {
                plan = Arc::new(LogicalFilter::new(having.clone(), plan));
            }
            return Ok(plan);
        }

        let alias_rewrite = AliasRewriter;
        if let Some(expr) = stmt.where_clause {
            plan = Arc::new(LogicalFilter::new(expr, plan));
        }

        let mut agg_extractor = AggExtractor::new();

        for expr in &mut stmt.select_list {
            agg_extractor.visit_select_expr(expr);
        }

        for expr in &mut stmt.group_by {
            agg_extractor.visit_group_by_expr(expr);
            if agg_extractor.group_by_has_agg() {
                // GROUP BY clause cannot have aggregate functions
                return Err(LogicalPlanError::InvalidSQL);
            }
        }

        if let Some(having) = &mut stmt.having {
            agg_extractor.visit_having_expr(having);
            alias_rewrite.rewrite_expr(having);
        }

        let column_count = stmt.select_list.len();
        for node in &mut stmt.orderby {
            agg_extractor.visit_having_expr(&mut node.expr);
            alias_rewrite.rewrite_expr(&mut node.expr);
            // If the expression does not exist, add a new expression to select_list
            // For example,
            // In SQL: `select a from t order by b;`
            // A column(b) expression will be created to ensure that the above operators get the
            // correct binding
            if !stmt.select_list.iter().any(|expr| {
                if let BoundExpr::ExprWithAlias(alias) = expr {
                    (*alias.expr) == node.expr
                } else {
                    expr == &node.expr
                }
            }) {
                // ORDER BY items must appear in the select list if SELECT DISTINCT is specified
                if stmt.select_distinct {
                    return Err(LogicalPlanError::IllegalDistinctSQL);
                }
                stmt.select_list.push(node.expr.clone());
            }
        }

        if !stmt.group_by.is_empty() || agg_extractor.has_aggregate() || stmt.having.is_some() {
            agg_extractor.validate_illegal_column(&stmt.select_list, &stmt.orderby)?;

            if let Some(having) = &stmt.having {
                let dummy = vec![];
                let havings = vec![having.clone()];
                agg_extractor.validate_illegal_column(&havings, &dummy)?;
            }
        }

        if !agg_extractor.agg_calls.is_empty() || !agg_extractor.group_by_exprs.is_empty() {
            plan = Arc::new(LogicalAggregate::new(
                agg_extractor.agg_calls,
                agg_extractor.group_by_exprs,
                plan,
            ));
        }

        if stmt.having.is_some() {
            plan = Arc::new(LogicalFilter::new(stmt.having.unwrap(), plan));
        }

        let comparators = stmt.orderby;

        let need_addtional_projection = column_count != stmt.select_list.len();
        let mut project = None;
        if !stmt.select_list.is_empty() {
            plan = Arc::new(LogicalProjection::new(stmt.select_list, plan));
            project = Some(plan.clone());
        }

        if stmt.select_distinct {
            let project = project.clone().unwrap();
            let projection = project.as_logical_projection().unwrap();
            let project_expressions = projection.project_expressions();
            let distinct_exprs = project_expressions.to_vec();
            plan = Arc::new(LogicalAggregate::new(vec![], distinct_exprs, plan));
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

        // If the project expressions have changed due to order by
        // For example,
        // In SQL: `select a from t order by b;`
        // We need to add a new projection operator above the order by
        // To ensure that the final output is correct
        if need_addtional_projection {
            let project = project.unwrap();
            let projection = project.as_logical_projection().unwrap();
            let mut projection_list = Vec::with_capacity(column_count);
            let project_expressions = projection.project_expressions();
            for item in project_expressions.iter().take(column_count) {
                projection_list.push(item.clone());
            }
            plan = Arc::new(LogicalProjection::new(projection_list, plan));
        }
        Ok(plan)
    }

    #[allow(clippy::only_used_in_recursion)]
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
    group_by_has_agg: bool,
}

impl AggExtractor {
    fn new() -> Self {
        AggExtractor {
            agg_calls: vec![],
            group_by_exprs: vec![],
            group_by_has_agg: false,
        }
    }

    fn validate_illegal_column_inner(&mut self, expr: &BoundExpr) -> Result<(), LogicalPlanError> {
        use BoundExpr::*;
        // found identical select expr in group by exprs
        if self.group_by_exprs.iter().any(|e| e == expr) {
            return Ok(());
        }

        match expr {
            BinaryOp(bin_op) => {
                self.validate_illegal_column_inner(&bin_op.left_expr)?;
                self.validate_illegal_column_inner(&bin_op.right_expr)?;
            }
            UnaryOp(unary_op) => self.validate_illegal_column_inner(&unary_op.expr)?,
            TypeCast(type_cast) => self.validate_illegal_column_inner(&type_cast.expr)?,
            ExprWithAlias(e) => {
                self.validate_illegal_column_inner(&e.expr)?;
            }
            IsNull(isnull) => self.validate_illegal_column_inner(&isnull.expr)?,
            AggCall(_) | Constant(_) | InputRef(_) | Alias(_) => {}
            ColumnRef(_) => {
                return Err(LogicalPlanError::IllegalGroupBySQL(format!(r#"{}"#, expr)));
            }
        }
        Ok(())
    }

    /// Validate select exprs must appear in the GROUP BY clause or be used in an aggregate
    /// function. Need `visit_group_by_expr` to rewrite the group by alias first.
    fn validate_illegal_column(
        &mut self,
        select_exprs: &[BoundExpr],
        orderby_exprs: &[BoundOrderBy],
    ) -> Result<(), LogicalPlanError> {
        for expr in select_exprs {
            self.validate_illegal_column_inner(expr)?;
        }
        for expr in orderby_exprs {
            self.validate_illegal_column_inner(&expr.expr)?;
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
        self.agg_calls.extend_from_slice(&agg_calls);
    }

    fn visit_having_expr(&mut self, expr: &mut BoundExpr) {
        struct Visitor<'a> {
            calls: &'a mut Vec<BoundAggCall>,
        }

        impl<'a> ExprVisitor for Visitor<'a> {
            fn visit_agg_call(&mut self, agg: &BoundAggCall) {
                // This aggregate does not exist in the select list
                // We should insert new aggregate function into aggregate list
                if !self.calls.iter().any(|call| call == agg) {
                    self.calls.push(agg.clone());
                }
            }
        }

        let mut vis = Visitor {
            calls: &mut self.agg_calls,
        };
        vis.visit_expr(expr);
    }

    fn visit_group_by_expr(&mut self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::Alias(alias) => {
                if let BoundExpr::AggCall(_) = &(*alias.expr) {
                    self.group_by_has_agg = true;
                    return;
                }
                self.group_by_exprs.push((*alias.expr).clone());
            }
            BoundExpr::AggCall(_) => {
                // GROUP BY clause cannot have aggregate functions
                self.group_by_has_agg = true;
            }
            _ => self.group_by_exprs.push(expr.clone()),
        }
    }

    fn group_by_has_agg(&self) -> bool {
        self.group_by_has_agg
    }

    fn has_aggregate(&self) -> bool {
        !self.agg_calls.is_empty()
    }
}

/// Alias rewriter rewrites alias expressions into actual expressions
struct AliasRewriter;

impl ExprRewriter for AliasRewriter {
    fn rewrite_alias(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::Alias(alias) => {
                *expr = (*alias.expr).clone();
            }

            _ => unreachable!(),
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
#[allow(dead_code)]
struct AliasExtractor<'a> {
    select_list: &'a [BoundExpr],
}

#[allow(dead_code)]
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
    use crate::binder::{AggKind, BoundAlias, BoundBinaryOp, BoundColumnRef, BoundExprWithAlias};
    use crate::catalog::ColumnRefId;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};

    #[test]
    fn test_agg_extractor_validate_illegal_column() {
        let v2 = build_column_ref(1, "v2".to_string());

        // case1 sql: select v2 + 1 from t group by v2 + 1
        let v2_plus_1 = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: v2.clone().into(),
            right_expr: BoundExpr::Constant(DataValue::Int32(1)).into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        assert!(
            validate_illegal_column(&mut [v2_plus_1.clone()], &mut [v2_plus_1.clone()], &[])
                .is_ok()
        );

        // case2 sql: select v2 + 1, v1 from t group by v2 + 1
        let v1 = build_column_ref(0, "v1".to_string());
        assert!(validate_illegal_column(
            &mut [v2_plus_1.clone(), v1.clone()],
            &mut [v2_plus_1.clone()],
            &[],
        )
        .is_err());

        // case3 sql: select v2 + 1 as a, v1 as b from t group by a
        let v2_plus_1_alias_a = BoundExpr::ExprWithAlias(BoundExprWithAlias {
            expr: v2_plus_1.clone().into(),
            alias: "a".to_string(),
        });
        let v1_alias_b = BoundExpr::ExprWithAlias(BoundExprWithAlias {
            expr: v1.clone().into(),
            alias: "b".to_string(),
        });
        let alias_a = BoundExpr::Alias(BoundAlias {
            alias: "a".to_string(),
            expr: Box::new(v2_plus_1.clone()),
        });
        assert!(
            validate_illegal_column(&mut [v2_plus_1_alias_a, v1_alias_b], &mut [alias_a], &[])
                .is_err()
        );

        // case4 sql: select v2 + 2 + count(*) from t group by v2 + 1;
        let v2_plus_2 = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: v2.clone().into(),
            right_expr: BoundExpr::Constant(DataValue::Int32(2)).into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        let count_wildcard = BoundExpr::AggCall(BoundAggCall {
            kind: AggKind::Count,
            args: vec![],
            return_type: DataTypeKind::Int(None).not_null(),
        });
        let v2_puls_2_plus_count = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: v2_plus_2.into(),
            right_expr: count_wildcard.clone().into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        assert!(
            validate_illegal_column(&mut [v2_puls_2_plus_count], &mut [v2_plus_1], &[]).is_err()
        );

        // case5 sql: select v2 + count(*) from t group by v2 order by v1;
        let v2_plus_count_wildcard = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Plus,
            left_expr: v2.clone().into(),
            right_expr: count_wildcard.into(),
            return_type: Some(DataTypeKind::Int(None).not_null()),
        });
        let order_by_v1 = BoundOrderBy {
            expr: v1,
            descending: false,
        };
        assert!(
            validate_illegal_column(&mut [v2_plus_count_wildcard], &mut [v2], &[order_by_v1])
                .is_err()
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
        order_by_list: &[BoundOrderBy],
    ) -> Result<(), LogicalPlanError> {
        let mut extractor = AggExtractor::new();
        for expr in group_by_list {
            extractor.visit_group_by_expr(expr);
        }
        extractor.validate_illegal_column(select_list, order_by_list)?;
        Ok(())
    }
}
