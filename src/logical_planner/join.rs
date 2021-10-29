use super::*;
use crate::binder::BoundJoinOperator;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalJoinTable {
    pub table_plan: Box<LogicalPlan>,
    pub join_op: BoundJoinOperator,
}
/// The logical plan of join, it only records join tables and operators.
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested loop join or index join).
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalJoin {
    pub relation_plan: Box<LogicalPlan>,
    pub join_table_plans: Vec<LogicalJoinTable>,
}
