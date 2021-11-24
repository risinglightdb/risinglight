use super::*;
use crate::{binder::BoundOrderBy, logical_planner::LogicalOrder};

/// The physical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_order(&self, plan: LogicalOrder) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Order(PhysicalOrder {
            comparators: plan.comparators,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
}

impl PlanExplainable for PhysicalOrder {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "OrderBy: {:?}", self.comparators)?;
        self.child.explain(level + 1, f)
    }
}
