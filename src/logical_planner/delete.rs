use super::*;
use crate::binder::{BoundDelete, BoundTableRef};
use crate::optimizer::plan_nodes::{LogicalDelete, LogicalFilter};

impl LogicalPlaner {
    pub fn plan_delete(&self, stmt: BoundDelete) -> Result<PlanRef, LogicalPlanError> {
        if let BoundTableRef::BaseTableRef { ref ref_id, .. } = stmt.from_table {
            if let Some(expr) = stmt.where_clause {
                let child = self.plan_table_ref(&stmt.from_table, true, false)?;
                Ok(Rc::new(LogicalDelete::new(
                    *ref_id,
                    Rc::new(LogicalFilter::new(expr, child)),
                )))
            } else {
                panic!("delete whole table is not supported yet")
            }
        } else {
            panic!("unsupported table")
        }
    }
}
