// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use egg::CostFunction;

use super::*;
use crate::catalog::RootCatalogRef;

/// Plan optimizer.
pub struct Optimizer {
    catalog: RootCatalogRef,
    config: Config,
}

/// Optimizer configurations.
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub enable_range_filter_scan: bool,
    pub table_is_sorted_by_primary_key: bool,
}

impl Optimizer {
    /// Creates a new optimizer.
    pub fn new(catalog: RootCatalogRef, config: Config) -> Self {
        Self { catalog, config }
    }

    /// Optimize the given expression.
    pub fn optimize(&self, expr: &RecExpr) -> RecExpr {
        let mut expr = expr.clone();

        // define extra rules for some configurations
        let mut extra_rules = vec![];
        if self.config.enable_range_filter_scan {
            extra_rules.append(&mut rules::filter_scan_rule());
        }

        // 1. pushdown
        let mut best_cost = f32::MAX;
        // to prune costy nodes, we iterate multiple times and only keep the best one for each run.
        for _ in 0..3 {
            let runner = egg::Runner::<_, _, ()>::new(ExprAnalysis {
                catalog: self.catalog.clone(),
                config: self.config.clone(),
            })
            .with_expr(&expr)
            .with_iter_limit(6)
            .run(rules::STAGE1_RULES.iter().chain(&extra_rules));
            let cost_fn = cost::CostFn {
                egraph: &runner.egraph,
            };
            let extractor = egg::Extractor::new(&runner.egraph, cost_fn);
            let cost;
            (cost, expr) = extractor.find_best(runner.roots[0]);
            if cost >= best_cost {
                break;
            }
            best_cost = cost;
            // println!(
            //     "{}",
            //     crate::planner::Explain::of(&expr).with_costs(&costs(&expr))
            // );
        }

        // 2. join reorder and hashjoin
        let runner = egg::Runner::<_, _, ()>::new(ExprAnalysis {
            catalog: self.catalog.clone(),
            config: self.config.clone(),
        })
        .with_expr(&expr)
        .run(&*rules::STAGE2_RULES);
        let cost_fn = cost::CostFn {
            egraph: &runner.egraph,
        };
        let extractor = egg::Extractor::new(&runner.egraph, cost_fn);
        (_, expr) = extractor.find_best(runner.roots[0]);

        expr
    }

    /// Returns the cost for each node in the expression.
    pub fn costs(&self, expr: &RecExpr) -> Vec<f32> {
        let mut egraph = EGraph::default();
        // NOTE: we assume Expr node has the same Id in both EGraph and RecExpr.
        egraph.add_expr(expr);
        let mut cost_fn = cost::CostFn { egraph: &egraph };
        let mut costs = vec![0.0; expr.as_ref().len()];
        for (i, node) in expr.as_ref().iter().enumerate() {
            let cost = cost_fn.cost(node, |i| costs[usize::from(i)]);
            costs[i] = cost;
        }
        costs
    }
}