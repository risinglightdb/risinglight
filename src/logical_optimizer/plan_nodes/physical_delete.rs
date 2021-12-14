use std::fmt;

use crate::catalog::TableRefId;
use crate::logical_optimizer::plan_nodes::logical_delete::LogicalDelete;

/// The physical plan of `delete`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalDelete {
    pub table_ref_id: TableRefId,
    pub child: PlanRef,
}

impl PhysicalPlaner {
    pub fn plan_delete(&self, plan: LogicalDelete) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Delete(PhysicalDelete {
            table_ref_id: plan.table_ref_id,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }
}

impl fmt::Display for PhysicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDelete: table {}", self.table_ref_id.table_id)?;
    }
}
