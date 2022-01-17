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
        Ok(Rc::new(LogicalTableScan::new(
            scan.table_ref_id(),
            scan.column_ids().to_vec(),
            scan.column_descs().to_vec(),
            scan.with_row_handler(),
            scan.is_sorted(),
            Some(filter.expr().clone()),
        )))
    }
}
