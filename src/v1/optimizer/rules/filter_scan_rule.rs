// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::v1::optimizer::plan_nodes::{LogicalTableScan, PlanTreeNodeUnary};

pub struct FilterScanRule {}

impl Rule for FilterScanRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let filter = plan.as_logical_filter()?;
        let child = filter.child();
        let scan = child.as_logical_table_scan()?.clone();
        Ok(Arc::new(LogicalTableScan::new(
            scan.table_ref_id(),
            scan.column_ids().to_vec(),
            scan.column_descs().to_vec(),
            scan.with_row_handler(),
            scan.is_sorted(),
            Some(filter.expr().clone()),
        )))
    }
}
