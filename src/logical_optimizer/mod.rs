pub(crate) mod plan_rewriter;
use crate::binder::*;

mod heuristic;
pub(crate) mod plan_nodes;
mod rules;
use self::{
    plan_nodes::LogicalPlanRef,
    plan_rewriter::{
        arith_expr_simplification::ArithExprSimplification,
        bool_expr_simplification::BoolExprSimplification, constant_folding::ConstantFolding,
        constant_moving::ConstantMovingRule, PlanRewriter,
    },
};

/// The optimizer will do query optimization.
///
/// It will do both rule-based optimization (predicate pushdown, constant folding and common
/// expression extraction) , and cost-based optimization (Join reordering and join algorithm
/// selection). It takes LogicalPlan as input and returns a new LogicalPlan which could be used to
/// generate phyiscal plan.
#[derive(Default)]
pub struct Optimizer {}

impl Optimizer {
    pub fn optimize(&mut self, mut plan: LogicalPlanRef) -> LogicalPlanRef {
        // TODO: Add more optimization rules.
        plan = ConstantFolding.rewrite_plan(plan);
        plan = ArithExprSimplification.rewrite_plan(plan);
        plan = BoolExprSimplification.rewrite_plan(plan);
        plan = ConstantMovingRule.rewrite_plan(plan);

        plan
    }
}
