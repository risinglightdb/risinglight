pub use filter_join_rule::*;

use super::plan_nodes::PlanRef;
pub mod filter_join_rule;

pub trait Rule: Send + Sync + 'static {
    /// if the plan matches
    fn matches(&self, plan: PlanRef) -> Result<(), ()>;
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()>;
}

pub(super) type BoxedRule = Box<dyn Rule>;
