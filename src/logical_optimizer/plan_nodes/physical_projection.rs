use std::fmt;

use super::*;
use crate::binder::BoundExpr;

/// The physical plan of project operation.
#[derive(Debug, Clone)]
pub struct PhysicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl_plan_node!(PhysicalProjection, [child]);

impl fmt::Display for PhysicalProjection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalProjection: exprs {:?}",
            self.project_expressions
        )
    }
}
