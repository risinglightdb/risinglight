use super::*;
use crate::binder::BoundExpr;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalProjection {
    pub project_expressions: Vec<BoundExpr>,
    pub child: Box<LogicalPlan>,
}
