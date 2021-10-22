use super::*;
use crate::binder::BoundJoinOperator;

// The logical plan of join
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalJoin {
    pub left_plan: Box<LogicalPlan>,
    pub right_plan: Box<LogicalPlan>,
    pub join_op: BoundJoinOperator,
}
