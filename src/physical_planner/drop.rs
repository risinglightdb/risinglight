use super::*;
use crate::{binder::Object, logical_planner::LogicalDrop};

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
