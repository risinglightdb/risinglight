// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::v1::binder::BoundCreateTable;
use crate::v1::optimizer::plan_nodes::LogicalCreateTable;

impl LogicalPlaner {
    pub fn plan_create_table(&self, stmt: BoundCreateTable) -> Result<PlanRef, LogicalPlanError> {
        Ok(Arc::new(LogicalCreateTable::new(
            stmt.database_id,
            stmt.schema_id,
            stmt.table_name,
            stmt.columns,
            stmt.ordered_pk_ids,
        )))
    }
}
