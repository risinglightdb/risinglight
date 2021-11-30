use crate::logical_planner::LogicalPlanRef;

pub(super) trait Rule: Send {
    fn matches(&self, plan: LogicalPlanRef) -> bool;
    fn apply(&self, plan: LogicalPlanRef) -> LogicalPlanRef;
}

pub(super) type BoxedRule = Box<dyn Rule>;
