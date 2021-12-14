use std::fmt;

use super::PlanRef;
use crate::binder::BoundExpr;

/// The physical plan of filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalFilter {
    pub expr: BoundExpr,
    pub child: PlanRef,
}

impl fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.expr)?;
    }
}
