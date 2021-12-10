use super::plan_nodes::LogicalPlanRef;
pub use filter_join_rule::*;
pub mod filter_join_rule;

pub trait Rule: Send + Sync + 'static {
    /// if the plan matches
    fn matches(&self, plan: LogicalPlanRef) -> Result<(), ()>;
    fn apply(&self, plan: LogicalPlanRef) -> Result<LogicalPlanRef, ()>;
}

pub(super) type BoxedRule = Box<dyn Rule>;
