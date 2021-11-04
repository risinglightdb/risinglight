use super::*;
use crate::catalog::TableRefId;
use crate::logical_planner::LogicalDelete;

/// The physical plan of `delete`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalDelete {
    pub table_ref_id: TableRefId,
    pub filter: Box<PhysicalPlan>,
}

impl PhysicalPlaner {
    pub fn plan_delete(&self, plan: LogicalDelete) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Delete(PhysicalDelete {
            table_ref_id: plan.table_ref_id,
            filter: Box::new(self.plan_filter(plan.filter)?),
        }))
    }
}

impl PlanExplainable for PhysicalDelete {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "DeleteTable: table {}", self.table_ref_id.table_id)?;
        self.filter.explain(level + 1, f)
    }
}
