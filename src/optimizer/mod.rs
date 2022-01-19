// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use crate::binder::*;

mod expr_utils;
mod heuristic;
pub mod logical_plan_rewriter;
pub mod plan_nodes;
mod plan_visitor;
mod rules;

use self::heuristic::HeuristicOptimizer;
use self::logical_plan_rewriter::*;
use self::plan_nodes::PlanRef;
pub use self::plan_visitor::*;
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
        // plan = plan.rewrite(&mut ConstantFolding);
        // plan = plan.rewrite(&mut ArithExprSimplification);
        // plan = plan.rewrite(&mut BoolExprSimplification);
        // plan = plan.rewrite(&mut ConstantMovingRule);
        let mut rules: Vec<Box<(dyn rules::Rule + 'static)>> = vec![Box::new(FilterJoinRule {})];
        if self.enable_filter_scan {
            rules.push(Box::new(FilterScanRule {}));
        }
        let hep_optimizer = HeuristicOptimizer { rules };
        plan = hep_optimizer.optimize(plan);
        let mut phy_converter = PhysicalConverter;
        phy_converter.rewrite(plan)
    }
}
