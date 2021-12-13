use std::fmt;

use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::logical_projection::LogicalProjection;
use crate::physical_planner::*;

/// The physical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl PhysicalPlaner {
    pub fn plan_projection(
        &self,
        plan: LogicalProjection,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Projection(PhysicalProjection {
            project_expressions: plan.project_expressions,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
}

impl fmt::Display for PhysicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalProjection: exprs {:?}",
            self.project_expressions
        )?;
    }
}
