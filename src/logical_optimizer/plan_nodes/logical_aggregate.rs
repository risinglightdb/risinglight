use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryLogicalPlanNode};
use crate::binder::{BoundAggCall, BoundExpr};

/// The logical plan of hash aggregate operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalAggregate {
    /// Filled by `InputRefResolver` in physical planner
    pub agg_calls: Vec<BoundAggCall>,
    /// Group keys in hash aggregation (optional)
    pub group_keys: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for LogicalAggregate {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::LogicalAggregate(LogicalAggregate {
            child,
            agg_calls: self.agg_calls.clone(),
            group_keys: self.group_keys.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {LogicalAggregate}

impl fmt::Display for LogicalAggregate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalAggregate: {} agg calls", self.agg_calls.len(),)
    }
}
