use super::*;
use crate::binder::{BoundAggCall, BoundExpr};

/// The logical plan of hash aggregate operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalAggregate {
    /// Filled by `InputRefResolver` in physical planner
    pub agg_calls: Vec<BoundAggCall>,
    /// Group keys in hash aggregation (optional)
    pub group_keys: Vec<BoundExpr>,
    pub child: LogicalPlanRef,
}
