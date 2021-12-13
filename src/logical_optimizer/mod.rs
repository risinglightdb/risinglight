pub(crate) mod plan_rewriter;
use crate::binder::*;
mod expr_utils;
mod heuristic;
use heuristic::HeuristicOptimizer;
pub(crate) mod plan_nodes;
mod rules;
use rules::*;

use self::plan_nodes::PlanRef;
use self::plan_rewriter::arith_expr_simplification::ArithExprSimplification;
use self::plan_rewriter::bool_expr_simplification::BoolExprSimplification;
use self::plan_rewriter::constant_folding::ConstantFolding;
use self::plan_rewriter::constant_moving::ConstantMovingRule;
use self::plan_rewriter::PlanRewriter;

/// The optimizer will do query optimization.
///
/// It will do both rule-based optimization (predicate pushdown, constant folding and common
/// expression extraction) , and cost-based optimization (Join reordering and join algorithm
/// selection). It takes Plan as input and returns a new Plan which could be used to
/// generate phyiscal plan.
#[derive(Default)]
pub struct Optimizer {}

impl Optimizer {
    pub fn optimize(&mut self, mut plan: PlanRef) -> PlanRef {
        // TODO: Add more optimization rules.
        plan = ConstantFolding.rewrite_plan(plan);
        plan = ArithExprSimplification.rewrite_plan(plan);
        plan = BoolExprSimplification.rewrite_plan(plan);
        plan = ConstantMovingRule.rewrite_plan(plan);
        let hep_optimizer = HeuristicOptimizer {
            rules: vec![Box::new(FilterJoinRule {})],
        };
        hep_optimizer.optimize(plan)
    }
}
