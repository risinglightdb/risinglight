use std::fmt;

use super::*;
use crate::binder::BoundOrderBy;

/// The physical plan of order.
#[derive(Debug, Clone)]
pub struct PhysicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: PlanRef,
}

impl_plan_node!(PhysicalOrder, [child]);

impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: {:?}", self.comparators)
    }
}
