use std::fmt;

use super::*;
use crate::binder::BoundJoinOperator;
/// The phyiscal plan of join.
#[derive(Clone, Debug)]
pub struct PhysicalHashJoin {
    pub left_plan: PlanRef,
    pub right_plan: PlanRef,
    pub join_op: BoundJoinOperator,
    pub condition: BoundExpr,
    pub left_column_index: usize,
    pub right_column_index: usize,
    data_types: Vec<DataType>,
}

impl PhysicalHashJoin {
    pub fn new(
        left_plan: PlanRef,
        right_plan: PlanRef,
        join_op: BoundJoinOperator,
        condition: BoundExpr,
        left_column_index: usize,
        right_column_index: usize,
    ) -> Self {
        let mut data_types = left_plan.out_types();
        data_types.append(&mut right_plan.out_types());
        PhysicalHashJoin {
            left_plan,
            right_plan,
            join_op,
            condition,
            left_column_index,
            right_column_index,
            data_types,
        }
    }
}

impl_plan_tree_node!(PhysicalHashJoin, [left_plan, right_plan]);
impl PlanNode for PhysicalHashJoin {
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}
/// Currently, we only use default join ordering.
/// We will implement DP or DFS algorithms for join orders.
impl fmt::Display for PhysicalHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashJoin: op {:?}", self.join_op)
    }
}
