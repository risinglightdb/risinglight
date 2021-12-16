use super::plan_nodes::logical_aggregate::LogicalAggregate;
use super::plan_nodes::logical_copy_from_file::LogicalCopyFromFile;
use super::plan_nodes::logical_copy_to_file::LogicalCopyToFile;
use super::plan_nodes::logical_create_table::LogicalCreateTable;
use super::plan_nodes::logical_delete::LogicalDelete;
use super::plan_nodes::logical_drop::LogicalDrop;
use super::plan_nodes::logical_explain::LogicalExplain;
use super::plan_nodes::logical_filter::LogicalFilter;
use super::plan_nodes::logical_insert::LogicalInsert;
use super::plan_nodes::logical_join::LogicalJoin;
use super::plan_nodes::logical_limit::LogicalLimit;
use super::plan_nodes::logical_order::LogicalOrder;
use super::plan_nodes::logical_projection::LogicalProjection;
use super::plan_nodes::logical_seq_scan::LogicalSeqScan;
use super::plan_nodes::logical_values::LogicalValues;
use super::plan_nodes::{Plan, PlanRef, UnaryPlanNode};
use crate::binder::{BoundAggCall, BoundExpr, BoundOrderBy};

pub(super) mod arith_expr_simplification;
pub(super) mod bool_expr_simplification;
pub(super) mod constant_folding;
pub(super) mod constant_moving;
pub(super) mod convert_physical;
pub mod input_ref_resolver;

// PlanRewriter is a plan visitor.
// User could implement the own optimization rules by implement PlanRewriter trait easily.
// NOTE: the visitor should always visit child plan first.
pub trait LogicalPlanRewriter {
    fn rewrite_plan(&mut self, plan: PlanRef) -> PlanRef {
        match self.rewrite_plan_inner(plan.clone()) {
            Some(new_plan) => new_plan,
            None => plan,
        }
    }

    // If the node do not need rewrite, return None.
    fn rewrite_plan_inner(&mut self, plan: PlanRef) -> Option<PlanRef> {
        match plan.as_ref() {
            Plan::Dummy(_) => None,
            Plan::LogicalCreateTable(plan) => self.rewrite_create_table(plan),
            Plan::LogicalDrop(plan) => self.rewrite_drop(plan),
            Plan::LogicalInsert(plan) => self.rewrite_insert(plan),
            Plan::LogicalJoin(plan) => self.rewrite_join(plan),
            Plan::LogicalSeqScan(plan) => self.rewrite_seqscan(plan),
            Plan::LogicalProjection(plan) => self.rewrite_projection(plan),
            Plan::LogicalFilter(plan) => self.rewrite_filter(plan),
            Plan::LogicalOrder(plan) => self.rewrite_order(plan),
            Plan::LogicalLimit(plan) => self.rewrite_limit(plan),
            Plan::LogicalExplain(plan) => self.rewrite_explain(plan),
            Plan::LogicalAggregate(plan) => self.rewrite_aggregate(plan),
            Plan::LogicalDelete(plan) => self.rewrite_delete(plan),
            Plan::LogicalValues(plan) => self.rewrite_values(plan),
            Plan::LogicalCopyFromFile(plan) => self.rewrite_copy_from_file(plan),
            Plan::LogicalCopyToFile(plan) => self.rewrite_copy_to_file(plan),
            _ => panic!("unsupported plan for visitor  "),
        }
    }

    fn rewrite_create_table(&mut self, _plan: &LogicalCreateTable) -> Option<PlanRef> {
        None
    }

    fn rewrite_drop(&mut self, _plan: &LogicalDrop) -> Option<PlanRef> {
        None
    }

    fn rewrite_insert(&mut self, plan: &LogicalInsert) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_join(&mut self, plan: &LogicalJoin) -> Option<PlanRef> {
        use super::BoundJoinConstraint::*;
        use super::BoundJoinOperator::*;

        Some(
            Plan::LogicalJoin(LogicalJoin {
                left_plan: self.rewrite_plan(plan.left_plan.clone()),
                right_plan: self.rewrite_plan(plan.right_plan.clone()),
                join_op: match plan.join_op.clone() {
                    Inner(On(expr)) => Inner(On(self.rewrite_expr(expr))),
                    LeftOuter(On(expr)) => LeftOuter(On(self.rewrite_expr(expr))),
                    RightOuter(On(expr)) => RightOuter(On(self.rewrite_expr(expr))),
                    CrossJoin => CrossJoin,
                },
            })
            .into(),
        )
    }

    fn rewrite_seqscan(&mut self, _plan: &LogicalSeqScan) -> Option<PlanRef> {
        None
    }

    fn rewrite_projection(&mut self, plan: &LogicalProjection) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::LogicalProjection(LogicalProjection {
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

    fn rewrite_filter(&mut self, plan: &LogicalFilter) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::LogicalFilter(LogicalFilter {
                child,
                expr: self.rewrite_expr(plan.expr.clone()),
            })
            .into(),
        )
    }

    fn rewrite_order(&mut self, plan: &LogicalOrder) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::LogicalOrder(LogicalOrder {
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

    fn rewrite_limit(&mut self, plan: &LogicalLimit) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_explain(&mut self, plan: &LogicalExplain) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_aggregate(&mut self, plan: &LogicalAggregate) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::LogicalAggregate(LogicalAggregate {
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

    fn rewrite_delete(&mut self, plan: &LogicalDelete) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_values(&mut self, _plan: &LogicalValues) -> Option<PlanRef> {
        None
    }

    fn rewrite_copy_from_file(&mut self, _plan: &LogicalCopyFromFile) -> Option<PlanRef> {
        None
    }

    fn rewrite_copy_to_file(&mut self, plan: &LogicalCopyToFile) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        expr
    }
}
