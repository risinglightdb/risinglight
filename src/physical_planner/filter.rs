use super::*;
use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::logical_filter::LogicalFilter;

/// The physical plan of filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalFilter {
    pub expr: BoundExpr,
    pub child: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_filter(&self, plan: LogicalFilter) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Filter(PhysicalFilter {
            expr: plan.expr,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
}

impl PlanExplainable for PhysicalFilter {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Filter: expr {:?}", self.expr)?;
        self.child.explain(level + 1, f)
    }
}
