// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::v1::binder::BoundInsert;
use crate::v1::optimizer::plan_nodes::{LogicalInsert, LogicalValues};

impl LogicalPlaner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<PlanRef, LogicalPlanError> {
        match stmt.select_stmt {
            Some(bound_select) => {
                let select_plan = self.plan_select(Box::new(*bound_select))?;
                Ok(Arc::new(LogicalInsert::new(
                    stmt.table_ref_id,
                    stmt.column_ids,
                    select_plan,
                )))
            }
            None => Ok(Arc::new(LogicalInsert::new(
                stmt.table_ref_id,
                stmt.column_ids,
                Arc::new(LogicalValues::new(stmt.column_types, stmt.values)),
            ))),
        }
    }
}
