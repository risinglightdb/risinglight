use super::*;
use crate::binder::BoundAggCall;

/// The logical plan of simple aggregate operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: Box<LogicalPlan>,
}
