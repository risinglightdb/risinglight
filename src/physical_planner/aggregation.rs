use super::*;
use crate::binder::{AggKind, BoundExpr};
use crate::logical_planner::LogicalAggregation;

#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalAggregation {
    pub agg_kind: Vec<AggKind>,
    pub aggregation_expressions: Vec<BoundExpr>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_aggregation(
        &self,
        plan: LogicalAggregation,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Aggregation(PhysicalAggregation {
            agg_kind: plan.agg_kind,
            aggregation_expressions: plan.aggregation_expressions,
            child: Box::new(self.plan(*plan.child)?),
        }))
    }
}
