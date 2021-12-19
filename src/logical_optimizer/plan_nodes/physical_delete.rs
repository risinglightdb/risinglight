use std::fmt;

use super::*;
use crate::catalog::TableRefId;

/// The physical plan of `DELETE`.
#[derive(Debug, Clone)]
pub struct PhysicalDelete {
    pub table_ref_id: TableRefId,
    pub child: PlanRef,
}

impl_plan_node!(PhysicalDelete, [child]);

impl fmt::Display for PhysicalDelete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalDelete: table {}", self.table_ref_id.table_id)
    }
}
