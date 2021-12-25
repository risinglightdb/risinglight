use crate::binder::*;

mod expr_utils;
mod heuristic;
pub mod logical_plan_rewriter;
pub mod plan_nodes;
mod rules;

use self::heuristic::HeuristicOptimizer;
use self::logical_plan_rewriter::*;
use self::plan_nodes::PlanRef;
use self::rules::*;

/// The optimizer will do query optimization.
///
/// It will do both rule-based optimization (predicate pushdown, constant folding and common
/// expression extraction) , and cost-based optimization (Join reordering and join algorithm
/// selection). It takes Plan as input and returns a new Plan which could be used to
/// generate phyiscal plan.
#[derive(Default)]
pub struct Optimizer {
    pub enable_filter_scan: bool,
}

impl Optimizer {
    pub fn optimize(&mut self, mut plan: PlanRef) -> PlanRef {
        // TODO: Add more optimization rules.
        plan = plan.rewrite(&mut ConstantFolding);
        plan = plan.rewrite(&mut ArithExprSimplification);
        plan = plan.rewrite(&mut BoolExprSimplification);
        plan = plan.rewrite(&mut ConstantMovingRule);
        let hep_optimizer = if self.enable_filter_scan {
            HeuristicOptimizer {
                rules: vec![Box::new(FilterJoinRule {}), Box::new(FilterScanRule {})]
            }
        } else {
            HeuristicOptimizer {
                rules: vec![Box::new(FilterJoinRule {})]
            }
        };
        plan = hep_optimizer.optimize(plan);
        plan.rewrite(&mut PhysicalConverter)
    }
}
