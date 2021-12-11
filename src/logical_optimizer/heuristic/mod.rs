use itertools::Itertools;

use super::plan_nodes::LogicalPlanRef;
use super::rules::BoxedRule;
pub struct HeuristicOptimizer {
    pub rules: Vec<BoxedRule>,
}

impl HeuristicOptimizer {
    pub fn optimize(&self, mut root: LogicalPlanRef) -> LogicalPlanRef {
        for rule in &self.rules {
            if rule.matches(root.clone()).is_ok() {
                if let Ok(applied) = rule.apply(root.clone()) {
                    root = applied;
                    break;
                }
            }
        }
        let children = root
            .children()
            .into_iter()
            .map(|sub_tree| self.optimize(sub_tree))
            .collect_vec();
        root.clone_with_children(children)
    }
}
