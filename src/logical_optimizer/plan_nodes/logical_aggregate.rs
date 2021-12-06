use super::{LogicalPlan, LogicalPlanRef, UnaryLogicalPlanNode};
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

impl UnaryLogicalPlanNode for LogicalAggregate {
    fn get_child(&self) -> LogicalPlanRef {
        self.child.clone()
    }

    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef {
        LogicalPlan::LogicalAggregate(LogicalAggregate {
            child,
            agg_calls: self.agg_calls.clone(),
            group_keys: self.group_keys.clone(),
        })
        .into()
    }
}
