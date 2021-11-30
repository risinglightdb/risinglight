use itertools::Itertools;

use crate::logical_planner::LogicalPlanRef;

use super::rules::BoxedRule;
#[allow(dead_code)]
struct HeuristicOptimizer {
    rules: Vec<BoxedRule>,
}
#[allow(dead_code)]

impl HeuristicOptimizer {
    fn optimize(&self, mut root: LogicalPlanRef) -> LogicalPlanRef {
        for rule in self.rules.iter() {
            if rule.matches(root.clone()) {
                root = rule.apply(root);
                // we will not try to apply rules on a new node after a rule applyed
                break;
            }
        }
        let children = root
            .get_children()
            .into_iter()
            .map(|sub_tree| self.optimize(sub_tree))
            .collect_vec();
        root.copy_with_children(children)
    }
}
