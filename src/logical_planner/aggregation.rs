use super::*;
use crate::binder::BoundExpr;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalAggregation {
    pub aggregation_expressions: Vec<BoundExpr>,
    pub child: Box<LogicalPlan>,
}
