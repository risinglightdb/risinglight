use std::fmt;

use super::*;
use crate::catalog::TableRefId;

/// The logical plan of `DELETE`.
#[derive(Debug, Clone)]
pub struct LogicalDelete {
    pub table_ref_id: TableRefId,
    pub child: PlanRef,
}

impl_plan_node!(LogicalDelete, [child]);

impl fmt::Display for LogicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDelete: table {}", self.table_ref_id.table_id)
    }
}
