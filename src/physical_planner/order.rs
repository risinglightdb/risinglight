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
            child: Box::new(self.plan(*plan.child)?),
        }))
    }
}

impl PlanExplainable for PhysicalOrder {
    fn explain_inner(&self, _level: usize,f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
