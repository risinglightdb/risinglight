use super::plan_nodes::physical_aggregate::PhysicalAggregate;
use super::plan_nodes::physical_copy_from_file::PhysicalCopyFromFile;
use super::plan_nodes::physical_copy_to_file::PhysicalCopyToFile;
use super::plan_nodes::physical_create_table::PhysicalCreateTable;
use super::plan_nodes::physical_delete::PhysicalDelete;
use super::plan_nodes::physical_drop::PhysicalDrop;
use super::plan_nodes::physical_explain::PhysicalExplain;
use super::plan_nodes::physical_filter::PhysicalFilter;
use super::plan_nodes::physical_insert::PhysicalInsert;
use super::plan_nodes::physical_join::PhysicalJoin;
use super::plan_nodes::physical_limit::PhysicalLimit;
use super::plan_nodes::physical_order::PhysicalOrder;
use super::plan_nodes::physical_projection::PhysicalProjection;
use super::plan_nodes::physical_seq_scan::PhysicalSeqScan;
use super::plan_nodes::physical_values::PhysicalValues;
use super::plan_nodes::{Plan, PlanRef, UnaryPhysicalPlanNode};
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
pub trait PhysicalPlanRewriter {
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
            Plan::PhysicalCreateTable(plan) => self.rewrite_create_table(plan),
            Plan::PhysicalDrop(plan) => self.rewrite_drop(plan),
            Plan::PhysicalInsert(plan) => self.rewrite_insert(plan),
            Plan::PhysicalJoin(plan) => self.rewrite_join(plan),
            Plan::PhysicalSeqScan(plan) => self.rewrite_seqscan(plan),
            Plan::PhysicalProjection(plan) => self.rewrite_projection(plan),
            Plan::PhysicalFilter(plan) => self.rewrite_filter(plan),
            Plan::PhysicalOrder(plan) => self.rewrite_order(plan),
            Plan::PhysicalLimit(plan) => self.rewrite_limit(plan),
            Plan::PhysicalExplain(plan) => self.rewrite_explain(plan),
            Plan::PhysicalAggregate(plan) => self.rewrite_aggregate(plan),
            Plan::PhysicalDelete(plan) => self.rewrite_delete(plan),
            Plan::PhysicalValues(plan) => self.rewrite_values(plan),
            Plan::PhysicalCopyFromFile(plan) => self.rewrite_copy_from_file(plan),
            Plan::PhysicalCopyToFile(plan) => self.rewrite_copy_to_file(plan),
            _ => panic!("unsupported plan for visitor  "),
        }
    }

    fn rewrite_create_table(&mut self, _plan: &PhysicalCreateTable) -> Option<PlanRef> {
        None
    }

    fn rewrite_drop(&mut self, _plan: &PhysicalDrop) -> Option<PlanRef> {
        None
    }

    fn rewrite_insert(&mut self, plan: &PhysicalInsert) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_join(&mut self, plan: &PhysicalJoin) -> Option<PlanRef> {
        use super::BoundJoinConstraint::*;
        use super::BoundJoinOperator::*;

        Some(
            Plan::PhysicalJoin(PhysicalJoin {
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

    fn rewrite_seqscan(&mut self, _plan: &PhysicalSeqScan) -> Option<PlanRef> {
        None
    }

    fn rewrite_projection(&mut self, plan: &PhysicalProjection) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::PhysicalProjection(PhysicalProjection {
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

    fn rewrite_filter(&mut self, plan: &PhysicalFilter) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::PhysicalFilter(PhysicalFilter {
                child,
                expr: self.rewrite_expr(plan.expr.clone()),
            })
            .into(),
        )
    }

    fn rewrite_order(&mut self, plan: &PhysicalOrder) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::PhysicalOrder(PhysicalOrder {
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

    fn rewrite_limit(&mut self, plan: &PhysicalLimit) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_explain(&mut self, plan: &PhysicalExplain) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_aggregate(&mut self, plan: &PhysicalAggregate) -> Option<PlanRef> {
        let child = self.rewrite_plan(plan.child());
        Some(
            Plan::PhysicalAggregate(PhysicalAggregate {
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

    fn rewrite_delete(&mut self, plan: &PhysicalDelete) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_values(&mut self, _plan: &PhysicalValues) -> Option<PlanRef> {
        None
    }

    fn rewrite_copy_from_file(&mut self, _plan: &PhysicalCopyFromFile) -> Option<PlanRef> {
        None
    }

    fn rewrite_copy_to_file(&mut self, plan: &PhysicalCopyToFile) -> Option<PlanRef> {
        if let Some(child) = self.rewrite_plan_inner(plan.child()) {
            return Some(plan.clone_with_child(child));
        }
        None
    }

    fn rewrite_expr(&mut self, expr: BoundExpr) -> BoundExpr {
        expr
    }
}
