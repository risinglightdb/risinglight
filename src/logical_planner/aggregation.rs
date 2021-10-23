use super::*;
use crate::binder::{AggKind, BoundExpr};

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalAggregation {
    pub agg_kind: Vec<AggKind>,
    pub aggregation_expressions: Vec<BoundExpr>,
    pub child: Box<LogicalPlan>,
}
