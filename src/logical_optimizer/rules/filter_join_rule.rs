use super::LogicalPlanRef;
use super::Rule;
use crate::logical_optimizer::plan_nodes::try_into_logicalfilter;
struct FilterJoinRule {}
impl Rule for FilterJoinRule {
    fn matches(&self, plan: LogicalPlanRef) -> bool {
        let filter = try_into_logicalfilter(plan.as_ref());
        todo!()
    }
    fn apply(&self, plan: LogicalPlanRef) -> LogicalPlanRef {
        todo!()
    }
}
