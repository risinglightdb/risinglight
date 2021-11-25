use itertools::Itertools;

use super::*;
use crate::{binder::BoundJoinOperator, logical_optimizer::plan_node::LogicalPlanNode};

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalJoinTable {
    pub table_plan: LogicalPlanRef,
    pub join_op: BoundJoinOperator,
}
/// The logical plan of join, it only records join tables and operators.
/// The query optimizer should decide the join orders and specific algorithms (hash join, nested
/// loop join or index join).
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalJoin {
    pub relation_plan: LogicalPlanRef,
    pub join_table_plans: Vec<LogicalJoinTable>,
}

impl LogicalPlanNode for LogicalJoin {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.relation_plan.clone()]
            .into_iter()
            .chain(
                self.join_table_plans
                    .iter()
                    .map(|join_table| join_table.table_plan.clone()),
            )
            .collect_vec()
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        let mut children_iter = children.into_iter();
        let relation_plan = children_iter.next().unwrap();
        let mut join_table_plans = self.join_table_plans.clone();
        join_table_plans
            .iter_mut()
            .zip_eq(children_iter)
            .for_each(|(join_table, table_plan)| {
                join_table.table_plan = table_plan;
            });
        LogicalPlan::LogicalJoin(LogicalJoin {
            relation_plan,
            join_table_plans,
        })
        .into()
    }
}
