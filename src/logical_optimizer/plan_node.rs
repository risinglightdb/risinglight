use crate::logical_planner::*;

pub trait LogicalPlanNode {
    fn get_children(&self) -> Vec<LogicalPlanRef>;
    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef;
}

// use marco to represent negative trait bounds
pub trait LeafLogicalPlanNode: Clone {}
macro_rules! impl_plan_node_for_leaf {
    ($leaf_node_type:ident) => {
        impl LogicalPlanNode for $leaf_node_type {
            fn get_children(&self) -> Vec<LogicalPlanRef> {
                vec![]
            }

            fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert!(children.is_empty());
                LogicalPlan::$leaf_node_type(self.clone()).into()
            }
        }
    };
}
pub trait UnaryLogicalPlanNode {
    fn get_child(&self) -> LogicalPlanRef;
    fn copy_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef;
}
macro_rules! impl_plan_node_for_unary {
    ($unary_node_type:ident) => {
        impl LogicalPlanNode for $unary_node_type {
            fn get_children(&self) -> Vec<LogicalPlanRef> {
                vec![self.get_child()]
            }

            fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert_eq!(children.len(), 1);
                self.copy_with_child(std::mem::take(&mut children[1]))
            }
        }
    };
}

impl_plan_node_for_leaf! {LogicalCreateTable}
impl_plan_node_for_leaf! {LogicalDrop}
impl_plan_node_for_leaf! {LogicalSeqScan}
impl_plan_node_for_leaf! {LogicalValues}
impl_plan_node_for_leaf! {LogicalCopyFromFile}

impl_plan_node_for_unary! {LogicalInsert}
impl_plan_node_for_unary! {LogicalAggregate}
impl_plan_node_for_unary! {LogicalProjection}
impl_plan_node_for_unary! {LogicalFilter}
impl_plan_node_for_unary! {LogicalOrder}
impl_plan_node_for_unary! {LogicalExplain}
impl_plan_node_for_unary! {LogicalLimit}
impl_plan_node_for_unary! {LogicalDelete}
impl_plan_node_for_unary! {LogicalCopyToFile}

// TODO: refactor with macro
#[allow(dead_code)]
impl LogicalPlan {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        match self {
            LogicalPlan::Dummy => vec![],
            LogicalPlan::LogicalCreateTable(plan) => plan.get_children(),
            LogicalPlan::LogicalDrop(plan) => plan.get_children(),
            LogicalPlan::LogicalInsert(plan) => plan.get_children(),
            LogicalPlan::LogicalJoin(plan) => plan.get_children(),
            LogicalPlan::LogicalSeqScan(plan) => plan.get_children(),
            LogicalPlan::LogicalProjection(plan) => plan.get_children(),
            LogicalPlan::LogicalFilter(plan) => plan.get_children(),
            LogicalPlan::LogicalOrder(plan) => plan.get_children(),
            LogicalPlan::LogicalLimit(plan) => plan.get_children(),
            LogicalPlan::LogicalExplain(plan) => plan.get_children(),
            LogicalPlan::LogicalAggregate(plan) => plan.get_children(),
            LogicalPlan::LogicalDelete(plan) => plan.get_children(),
            LogicalPlan::LogicalValues(plan) => plan.get_children(),
            LogicalPlan::LogicalCopyFromFile(plan) => plan.get_children(),
            LogicalPlan::LogicalCopyToFile(plan) => plan.get_children(),
        }
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        match self {
            LogicalPlan::Dummy => LogicalPlan::Dummy.into(),
            LogicalPlan::LogicalCreateTable(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalDrop(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalInsert(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalJoin(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalSeqScan(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalProjection(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalFilter(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalOrder(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalLimit(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalExplain(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalAggregate(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalDelete(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalValues(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalCopyFromFile(plan) => plan.copy_with_children(children),
            LogicalPlan::LogicalCopyToFile(plan) => plan.copy_with_children(children),
        }
    }
}
