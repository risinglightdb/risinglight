use std::fmt;

use super::PlanRef;

/// The physical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
}

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
