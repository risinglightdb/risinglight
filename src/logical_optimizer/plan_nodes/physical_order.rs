use std::fmt;

use super::PlanRef;
use crate::binder::BoundOrderBy;
use crate::logical_optimizer::plan_nodes::logical_order::LogicalOrder;

/// The physical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: PlanRef,
}

impl PhysicalPlaner {
    pub fn plan_order(&self, plan: LogicalOrder) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Order(PhysicalOrder {
            comparators: plan.comparators,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
}

impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: {:?}", self.comparators)?;
    }
}
