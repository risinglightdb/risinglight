use super::*;
use crate::binder::BoundInsert;
use crate::optimizer::plan_nodes::{LogicalInsert, LogicalValues};

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<PlanRef, LogicalPlanError> {
        Ok(Rc::new(LogicalInsert::new(
            stmt.table_ref_id,
            stmt.column_ids,
            Rc::new(LogicalValues::new(stmt.column_types, stmt.values)),
        )))
    }
}
