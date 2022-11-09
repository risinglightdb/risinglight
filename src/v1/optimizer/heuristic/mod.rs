// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;

use super::plan_nodes::PlanRef;
use super::rules::BoxedRule;

pub struct HeuristicOptimizer {
    pub rules: Vec<BoxedRule>,
}

impl HeuristicOptimizer {
    pub fn optimize(&self, mut root: PlanRef) -> PlanRef {
        for rule in &self.rules {
            if let Ok(applied) = rule.apply(root.clone()) {
                root = applied;
                break;
            }
        }
        let children = root
            .children()
            .into_iter()
            .map(|sub_tree| self.optimize(sub_tree))
            .collect_vec();
        root.clone_with_children(&children)
    }
}
