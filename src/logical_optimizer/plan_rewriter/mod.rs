use super::plan_nodes::{
    logical_aggregate::LogicalAggregate, logical_copy_from_file::LogicalCopyFromFile,
    logical_copy_to_file::LogicalCopyToFile, logical_create_table::LogicalCreateTable,
    logical_delete::LogicalDelete, logical_drop::LogicalDrop, logical_explain::LogicalExplain,
    logical_filter::LogicalFilter, logical_insert::LogicalInsert, logical_join::LogicalJoin,
    logical_limit::LogicalLimit, logical_order::LogicalOrder,
    logical_projection::LogicalProjection, logical_seq_scan::LogicalSeqScan,
    logical_values::LogicalValues, LogicalPlan, LogicalPlanRef, UnaryLogicalPlanNode,
};
use crate::binder::{BoundAggCall, BoundExpr, BoundOrderBy};

pub(super) mod arith_expr_simplification;
pub(super) mod bool_expr_simplification;
pub(super) mod constant_folding;
pub(super) mod constant_moving;
pub mod input_ref_resolver;

// PlanRewriter is a plan visitor.
// User could implement the own optimization rules by implement PlanRewriter trait easily.
// NOTE: the visitor should always visit child plan first.
pub trait PlanRewriter {
    fn rewrite_plan(&mut self, plan: LogicalPlanRef) -> LogicalPlanRef {
        match self.rewrite_plan_inner(plan.clone()) {
            Some(new_plan) => new_plan,
            None => plan,
        }
    }

    // If the node do not need rewrite, return None.
    fn rewrite_plan_inner(&mut self, plan: LogicalPlanRef) -> Option<LogicalPlanRef> {
        match plan.as_ref() {
            LogicalPlan::Dummy => None,
            LogicalPlan::LogicalCreateTable(plan) => self.rewrite_create_table(plan),
            LogicalPlan::LogicalDrop(plan) => self.rewrite_drop(plan),
            LogicalPlan::LogicalInsert(plan) => self.rewrite_insert(plan),
            LogicalPlan::LogicalJoin(plan) => self.rewrite_join(plan),
            LogicalPlan::LogicalSeqScan(plan) => self.rewrite_seqscan(plan),
            LogicalPlan::LogicalProjection(plan) => self.rewrite_projection(plan),
            LogicalPlan::LogicalFilter(plan) => self.rewrite_filter(plan),
            LogicalPlan::LogicalOrder(plan) => self.rewrite_order(plan),
            LogicalPlan::LogicalLimit(plan) => self.rewrite_limit(plan),
            LogicalPlan::LogicalExplain(plan) => self.rewrite_explain(plan),
            LogicalPlan::LogicalAggregate(plan) => self.rewrite_aggregate(plan),
            LogicalPlan::LogicalDelete(plan) => self.rewrite_delete(plan),
            LogicalPlan::LogicalValues(plan) => self.rewrite_values(plan),
            LogicalPlan::LogicalCopyFromFile(plan) => self.rewrite_copy_from_file(plan),
            LogicalPlan::LogicalCopyToFile(plan) => self.rewrite_copy_to_file(plan),
        }
    }

    fn rewrite_create_table(&mut self, _plan: &LogicalCreateTable) -> Option<LogicalPlanRef> {
        None
    }

    fn rewrite_drop(&mut self, _plan: &LogicalDrop) -> Option<LogicalPlanRef> {
        None
    }

    fn rewrite_insert(&mut self, plan: &LogicalInsert) -> Option<LogicalPlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.get_child()) {
            return Some(plan.copy_with_child(child));
        }
        None
    }

    fn rewrite_join(&mut self, plan: &LogicalJoin) -> Option<LogicalPlanRef> {
        use super::BoundJoinConstraint::*;
        use super::BoundJoinOperator::*;

        Some(
            LogicalPlan::LogicalJoin(LogicalJoin {
                left_plan: self.rewrite_plan(plan.left_plan.clone()),
                right_plan: self.rewrite_plan(plan.right_plan.clone()),
                join_op: match plan.join_op.clone() {
                    Inner(On(expr)) => Inner(On(self.rewrite_expr(expr))),
                    LeftOuter(On(expr)) => LeftOuter(On(self.rewrite_expr(expr))),
                    RightOuter(On(expr)) => RightOuter(On(self.rewrite_expr(expr))),
                },
            })
            .into(),
        )
    }

    fn rewrite_seqscan(&mut self, _plan: &LogicalSeqScan) -> Option<LogicalPlanRef> {
        None
    }

    fn rewrite_projection(&mut self, plan: &LogicalProjection) -> Option<LogicalPlanRef> {
        let child = self.rewrite_plan(plan.get_child());
        Some(
            LogicalPlan::LogicalProjection(LogicalProjection {
                child,
                project_expressions: plan
                    .project_expressions
                    .iter()
                    .cloned()
                    .map(|expr| self.rewrite_expr(expr))
                    .collect(),
            })
            .into(),
        )
    }

    fn rewrite_filter(&mut self, plan: &LogicalFilter) -> Option<LogicalPlanRef> {
        let child = self.rewrite_plan(plan.get_child());
        Some(
            LogicalPlan::LogicalFilter(LogicalFilter {
                child,
                expr: self.rewrite_expr(plan.expr.clone()),
            })
            .into(),
        )
    }

    fn rewrite_order(&mut self, plan: &LogicalOrder) -> Option<LogicalPlanRef> {
        let child = self.rewrite_plan(plan.get_child());
        Some(
            LogicalPlan::LogicalOrder(LogicalOrder {
                child,
                comparators: plan
                    .comparators
                    .iter()
                    .cloned()
                    .map(|orderby| BoundOrderBy {
                        expr: self.rewrite_expr(orderby.expr),
                        descending: orderby.descending,
                    })
                    .collect(),
            })
            .into(),
        )
    }

    fn rewrite_limit(&mut self, plan: &LogicalLimit) -> Option<LogicalPlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.get_child()) {
            return Some(plan.copy_with_child(child));
        }
        None
    }

    fn rewrite_explain(&mut self, plan: &LogicalExplain) -> Option<LogicalPlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.get_child()) {
            return Some(plan.copy_with_child(child));
        }
        None
    }

    fn rewrite_aggregate(&mut self, plan: &LogicalAggregate) -> Option<LogicalPlanRef> {
        let child = self.rewrite_plan(plan.get_child());
        Some(
            LogicalPlan::LogicalAggregate(LogicalAggregate {
                child,
                agg_calls: plan
                    .agg_calls
                    .iter()
                    .cloned()
                    .map(|agg| BoundAggCall {
                        kind: agg.kind,
                        args: agg
                            .args
                            .into_iter()
                            .map(|expr| self.rewrite_expr(expr))
                            .collect(),
                        return_type: agg.return_type,
                    })
                    .collect(),
                group_keys: plan.group_keys.clone(),
            })
            .into(),
        )
    }

    fn rewrite_delete(&mut self, plan: &LogicalDelete) -> Option<LogicalPlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.get_child()) {
            return Some(plan.copy_with_child(child));
        }
        None
    }

    fn rewrite_values(&mut self, _plan: &LogicalValues) -> Option<LogicalPlanRef> {
        None
    }

    fn rewrite_copy_from_file(&mut self, _plan: &LogicalCopyFromFile) -> Option<LogicalPlanRef> {
        None
    }

    fn rewrite_copy_to_file(&mut self, plan: &LogicalCopyToFile) -> Option<LogicalPlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.get_child()) {
            return Some(plan.copy_with_child(child));
        }
        None
    }

    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        expr
    }
}
