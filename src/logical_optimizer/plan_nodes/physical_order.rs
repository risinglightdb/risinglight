use std::fmt;

use super::PlanRef;
use crate::binder::BoundOrderBy;

/// The physical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: PlanRef,
}

impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: {:?}", self.comparators)?;
    }
}
