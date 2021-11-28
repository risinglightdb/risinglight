pub(crate) mod plan_rewriter;
use crate::{binder::*, logical_planner::*};
pub(crate) mod plan_node;

use self::plan_rewriter::{
    arith_expr_simplification::ArithExprSimplification,
    bool_expr_simplification::BoolExprSimplification, constant_folding::ConstantFolding,
    constant_moving::ConstantMovingRule, PlanRewriter,
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
    pub fn optimize(&mut self, plan: LogicalPlan) -> LogicalPlan {
        // TODO: Add more optimization rules.
        let mut plan = plan.into();

        plan = ConstantFolding.rewrite_plan(plan);
        plan = ArithExprSimplification.rewrite_plan(plan);
        plan = BoolExprSimplification.rewrite_plan(plan);
        plan = ConstantMovingRule.rewrite_plan(plan);

        plan.as_ref().clone()
    }
}
