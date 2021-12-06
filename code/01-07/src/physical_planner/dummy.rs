use super::*;
use crate::logical_planner::LogicalDummy;

#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalDummy;

impl PhysicalPlanner {
    pub fn plan_dummy(&self, _plan: &LogicalDummy) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalDummy.into())
    }
}

impl Explain for PhysicalDummy {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Dummy:")
    }
}
