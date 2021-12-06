use logical_copy_from_file::LogicalCopyFromFile;
use logical_copy_to_file::LogicalCopyToFile;
use logical_create_table::LogicalCreateTable;
use logical_delete::LogicalDelete;
use logical_drop::LogicalDrop;
use logical_explain::LogicalExplain;
use logical_insert::LogicalInsert;
use logical_values::LogicalValues;
use std::rc::Rc;

use self::{
    logical_aggregate::LogicalAggregate, logical_filter::LogicalFilter, logical_join::LogicalJoin,
    logical_limit::LogicalLimit, logical_order::LogicalOrder,
    logical_projection::LogicalProjection, logical_seq_scan::LogicalSeqScan,
};

pub(crate) mod logical_aggregate;
pub(crate) mod logical_copy_from_file;
pub(crate) mod logical_copy_to_file;
pub(crate) mod logical_create_table;
pub(crate) mod logical_delete;
pub(crate) mod logical_drop;
pub(crate) mod logical_explain;
pub(crate) mod logical_filter;
pub(crate) mod logical_insert;
pub(crate) mod logical_join;
pub(crate) mod logical_limit;
pub(crate) mod logical_order;
pub(crate) mod logical_projection;
pub(crate) mod logical_seq_scan;
pub(crate) mod logical_values;

pub(super) trait LogicalPlanNode {
    fn get_children(&self) -> Vec<LogicalPlanRef>;
    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef;
}

// use marco to represent negative trait bounds
pub(super) trait LeafLogicalPlanNode: Clone {}
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
pub(super) trait UnaryLogicalPlanNode {
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
                self.copy_with_child(children.pop().unwrap())
            }
        }
    };
}

pub trait BinaryLogicalPlanNode {
    fn get_left(&self) -> LogicalPlanRef;
    fn get_right(&self) -> LogicalPlanRef;
    fn copy_with_left_right(&self, left: LogicalPlanRef, right: LogicalPlanRef) -> LogicalPlanRef;
}
macro_rules! impl_plan_node_for_binary {
    ($binary_node_type:ident) => {
        impl LogicalPlanNode for $binary_node_type {
            fn get_children(&self) -> Vec<LogicalPlanRef> {
                vec![self.get_left(), self.get_right()]
            }

            fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert_eq!(children.len(), 2);
                let mut iter = children.into_iter();
                self.copy_with_left_right(iter.next().unwrap(), iter.next().unwrap())
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

impl_plan_node_for_binary! {LogicalJoin}

/// An enumeration which record all necessary information of execution plan,
/// which will be used by optimizer and executor.
pub(crate) type LogicalPlanRef = Rc<LogicalPlan>;
#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Dummy,
    LogicalSeqScan(LogicalSeqScan),
    LogicalInsert(LogicalInsert),
    LogicalValues(LogicalValues),
    LogicalCreateTable(LogicalCreateTable),
    LogicalDrop(LogicalDrop),
    LogicalProjection(LogicalProjection),
    LogicalFilter(LogicalFilter),
    LogicalExplain(LogicalExplain),
    LogicalJoin(LogicalJoin),
    LogicalAggregate(LogicalAggregate),
    LogicalOrder(LogicalOrder),
    LogicalLimit(LogicalLimit),
    LogicalDelete(LogicalDelete),
    LogicalCopyFromFile(LogicalCopyFromFile),
    LogicalCopyToFile(LogicalCopyToFile),
}

// TODO: refactor with macro
impl LogicalPlan {
    pub fn get_children(&self) -> Vec<LogicalPlanRef> {
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

    pub fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
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
