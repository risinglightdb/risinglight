// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::BoundInsert;
use crate::optimizer::plan_nodes::{LogicalInsert, LogicalValues};

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<PlanRef, LogicalPlanError> {
        Ok(Arc::new(LogicalInsert::new(
            stmt.table_ref_id,
            stmt.column_ids,
            Arc::new(LogicalValues::new(stmt.column_types, stmt.values)),
        )))
    }
}
