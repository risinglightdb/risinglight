use super::*;
use crate::optimizer::plan_nodes::{LogicalFilter, LogicalTableScan, PlanTreeNodeUnary};

pub struct FilterScanRule {}

impl Rule for FilterScanRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let filter = plan.downcast_rc::<LogicalFilter>().map_err(|_| ())?;
        let scan = filter
            .child()
            .clone()
            .downcast_rc::<LogicalTableScan>()
            .map_err(|_| ())?;
        Ok(Rc::new(LogicalTableScan {
            table_ref_id: scan.table_ref_id(),
            column_ids: scan.column_ids().to_vec(),
            column_descs: scan.column_descs().to_vec(),
            with_row_handler: scan.with_row_handler(),
            is_sorted: scan.is_sorted(),
            expr: Some(filter.expr().clone()),
        }))
    }
}
