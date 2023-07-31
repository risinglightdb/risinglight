// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::LazyLock;

use egg::CostFunction;

use super::*;
use crate::catalog::RootCatalogRef;

/// Plan optimizer.
#[derive(Clone)]
pub struct Optimizer {
    analysis: ExprAnalysis,
}

/// Optimizer configurations.
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub enable_range_filter_scan: bool,
    pub table_is_sorted_by_primary_key: bool,
}

impl Optimizer {
    /// Creates a new optimizer.
    pub fn new(catalog: RootCatalogRef, stat: Statistics, config: Config) -> Self {
        Self {
            analysis: ExprAnalysis {
                catalog,
                config,
                stat,
            },
        }
    }

    /// Optimize the given expression.
    pub fn optimize(&self, expr: &RecExpr) -> RecExpr {
        let mut expr = expr.clone();

        // define extra rules for some configurations
        let mut extra_rules = vec![];
        if self.analysis.config.enable_range_filter_scan {
            extra_rules.append(&mut rules::range::filter_scan_rule());
        }

        // 1. pushdown
        let mut best_cost = f32::MAX;
        // to prune costy nodes, we iterate multiple times and only keep the best one for each run.
        for _ in 0..3 {
            let runner = egg::Runner::<_, _, ()>::new(self.analysis.clone())
                .with_expr(&expr)
                .with_iter_limit(6)
                .run(STAGE1_RULES.iter().chain(&extra_rules));
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
            //     Explain::of(&expr)
            //         .with_costs(&self.costs(&expr))
            //         .with_rows(&self.rows(&expr))
            // );
        }

        // 2. join reorder and hashjoin
        for _ in 0..4 {
            let runner = egg::Runner::<_, _, ()>::new(self.analysis.clone())
                .with_expr(&expr)
                .with_iter_limit(8)
                .run(&*STAGE2_RULES);
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
            //     Explain::of(&expr)
            //         .with_costs(&self.costs(&expr))
            //         .with_rows(&self.rows(&expr))
            // );
        }

        expr
    }

    /// Returns the cost for each node in the expression.
    pub fn costs(&self, expr: &RecExpr) -> Vec<f32> {
        let mut egraph = EGraph::new(self.analysis.clone());
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

    /// Returns the estimated row for each node in the expression.
    pub fn rows(&self, expr: &RecExpr) -> Vec<f32> {
        let mut egraph = EGraph::new(self.analysis.clone());
        // NOTE: we assume Expr node has the same Id in both EGraph and RecExpr.
        egraph.add_expr(expr);
        (0..expr.as_ref().len())
            .map(|i| egraph[i.into()].data.rows)
            .collect()
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &RootCatalogRef {
        &self.analysis.catalog
    }
}

/// Stage1 rules in the optimizer.
static STAGE1_RULES: LazyLock<Vec<Rewrite>> = LazyLock::new(|| {
    let mut rules = vec![];
    rules.append(&mut rules::expr::rules());
    rules.append(&mut rules::plan::always_better_rules());
    rules.append(&mut rules::order::order_rules());
    rules
});

/// Stage2 rules in the optimizer.
static STAGE2_RULES: LazyLock<Vec<Rewrite>> = LazyLock::new(|| {
    let mut rules = vec![];
    rules.append(&mut rules::expr::and_rules());
    rules.append(&mut rules::plan::always_better_rules());
    rules.append(&mut rules::plan::join_reorder_rules());
    rules.append(&mut rules::plan::hash_join_rules());
    rules.append(&mut rules::order::order_rules());
    rules
});
