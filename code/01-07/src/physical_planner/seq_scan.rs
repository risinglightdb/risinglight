use super::*;
use crate::catalog::{ColumnId, TableRefId};
use crate::logical_planner::LogicalGet;

/// The physical plan of sequential scan operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSeqScan {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
}

impl PhysicalPlanner {
    pub fn plan_get(&self, plan: &LogicalGet) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalSeqScan {
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids.clone(),
        }
        .into())
    }
}

impl Explain for PhysicalSeqScan {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "SeqScan: table #{}, columns: {:?}",
            self.table_ref_id.table_id, self.column_ids,
        )
    }
}
