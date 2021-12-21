use super::*;
use crate::binder::BoundExpr;
use crate::logical_planner::LogicalProjection;

/// The physical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalProjection {
    pub exprs: Vec<BoundExpr>,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlanner {
    pub fn plan_projection(
        &self,
        plan: &LogicalProjection,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalProjection {
            exprs: plan.exprs.clone(),
            child: self.plan(&plan.child)?.into(),
        }
        .into())
    }
}

impl Explain for PhysicalProjection {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Projection: exprs: {:?}", self.exprs)?;
        self.child.explain(level + 1, f)
    }
}
