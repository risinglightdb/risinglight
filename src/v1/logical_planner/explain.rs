// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::v1::optimizer::plan_nodes::LogicalExplain;

impl LogicalPlaner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<PlanRef, LogicalPlanError> {
        Ok(Arc::new(LogicalExplain::new(self.plan(stmt)?)))
    }
}
