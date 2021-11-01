use super::*;
use crate::{binder::Object, logical_planner::LogicalDrop};

/// The physical plan of `drop`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalDrop {
    pub object: Object,
}

impl PhysicalPlaner {
    pub fn plan_drop(&self, plan: LogicalDrop) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Drop(PhysicalDrop {
            object: plan.object,
        }))
    }
}

impl PlanExplainable for PhysicalDrop {
    fn explain_inner(&self,_level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
