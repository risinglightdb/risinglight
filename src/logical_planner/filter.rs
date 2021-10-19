use super::*;
use crate::binder::BoundExpr;

/// The logical plan of filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalFilter {
    pub expr: BoundExpr,
    pub child: Box<LogicalPlan>,
}
