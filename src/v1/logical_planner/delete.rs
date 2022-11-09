// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::v1::binder::{BoundDelete, BoundTableRef};
use crate::v1::optimizer::plan_nodes::{LogicalDelete, LogicalFilter};

impl LogicalPlaner {
    pub fn plan_delete(&self, stmt: BoundDelete) -> Result<PlanRef, LogicalPlanError> {
        use crate::types::DataValue;
        use crate::v1::binder::BoundExpr;

        if let BoundTableRef::BaseTableRef { ref ref_id, .. } = stmt.from_table {
            if let Some(expr) = stmt.where_clause {
                let child = self.plan_table_ref(&stmt.from_table, true, false)?;
                Ok(Arc::new(LogicalDelete::new(
                    *ref_id,
                    Arc::new(LogicalFilter::new(expr, child)),
                )))
            } else {
                let mut stmt = stmt;
                stmt.where_clause = Some(BoundExpr::Constant(DataValue::Bool(true)));
                self.plan_delete(stmt)
            }
        } else {
            panic!("unsupported table")
        }
    }
}
