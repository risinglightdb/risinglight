use super::*;
use crate::{binder::BoundExpr, logical_planner::LogicalAggregation};

#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalAggregation {
    pub aggregation_expressions: Vec<BoundExpr>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_aggregation(
        &self,
        plan: LogicalAggregation,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Aggregation(PhysicalAggregation {
            aggregation_expressions: plan.aggregation_expressions,
            child: Box::new(self.plan(*plan.child)?),
        }))
    }
}
