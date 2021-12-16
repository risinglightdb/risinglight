use super::*;
use crate::binder::BoundInsert;
use crate::logical_optimizer::plan_nodes::logical_insert::LogicalInsert;
use crate::logical_optimizer::plan_nodes::logical_values::LogicalValues;

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<Plan, LogicalPlanError> {
        Ok(Plan::LogicalInsert(LogicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            child: Plan::LogicalValues(LogicalValues {
                column_types: stmt.column_types,
                values: stmt.values,
            })
            .into(),
        }))
    }
}
