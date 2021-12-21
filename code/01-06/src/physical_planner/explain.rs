use super::*;
use crate::logical_planner::LogicalExplain;

/// The physical plan of `EXPLAIN`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalExplain {
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlanner {
    pub fn plan_explain(&self, plan: &LogicalExplain) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalExplain {
            child: self.plan(&plan.child)?.into(),
        }
        .into())
    }
}

impl Explain for PhysicalExplain {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
