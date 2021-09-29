use super::*;
use crate::binder::BoundExpr;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalFilter {
    pub expr: BoundExpr,
    pub child: Box<LogicalPlan>,
}
