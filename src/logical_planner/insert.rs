use super::*;
use crate::binder::BoundInsert;
use crate::optimizer::plan_nodes::{LogicalInsert, LogicalValues};

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<PlanRef, LogicalPlanError> {
        Ok(Rc::new(LogicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            child: Rc::new(LogicalValues {
                column_types: stmt.column_types,
                values: stmt.values,
            }),
        }))
    }
}
