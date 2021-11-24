use super::*;
use crate::binder::BoundExpr;

/// The logical plan of project operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: LogicalPlanRef,
}
