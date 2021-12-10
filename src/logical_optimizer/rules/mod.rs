use super::plan_nodes::LogicalPlanRef;
pub use filter_join_rule::*;
pub mod filter_join_rule;

pub trait Rule: Send {
    fn matches(&self, plan: LogicalPlanRef) -> bool;
    fn apply(&self, plan: LogicalPlanRef) -> LogicalPlanRef;
}

pub(super) type BoxedRule = Box<dyn Rule>;
