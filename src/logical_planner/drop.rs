// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::binder::BoundDrop;
use crate::optimizer::plan_nodes::LogicalDrop;

impl LogicalPlaner {
    pub fn plan_drop(&self, stmt: BoundDrop) -> Result<PlanRef, LogicalPlanError> {
        Ok(Arc::new(LogicalDrop::new(stmt.object)))
    }
}
