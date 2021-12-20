use super::*;
use crate::binder::BoundCreateTable;
use crate::optimizer::plan_nodes::LogicalCreateTable;

impl LogicalPlaner {
    pub fn plan_create_table(&self, stmt: BoundCreateTable) -> Result<PlanRef, LogicalPlanError> {
        Ok(Rc::new(LogicalCreateTable {
            database_id: stmt.database_id,
            schema_id: stmt.schema_id,
            table_name: stmt.table_name,
            columns: stmt.columns,
        }))
    }
}
