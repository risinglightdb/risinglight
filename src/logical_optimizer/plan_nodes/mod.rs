use logical_copy_from_file::LogicalCopyFromFile;
use logical_copy_to_file::LogicalCopyToFile;
use logical_create_table::LogicalCreateTable;
use logical_delete::LogicalDelete;
use logical_drop::LogicalDrop;
use logical_explain::LogicalExplain;
use logical_insert::LogicalInsert;
use logical_values::LogicalValues;
use std::rc::Rc;

use crate::physical_planner::Dummy;

use self::{
    logical_aggregate::LogicalAggregate, logical_filter::LogicalFilter, logical_join::LogicalJoin,
    logical_limit::LogicalLimit, logical_order::LogicalOrder,
    logical_projection::LogicalProjection, logical_seq_scan::LogicalSeqScan,
};

pub(crate) mod dummy;
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
    fn children(&self) -> Vec<LogicalPlanRef>;
    fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef;
}

#[macro_export]
macro_rules! for_all_plan_nodes {
  ($macro:tt $(, $x:tt)*) => {
    $macro! {
      [$($x),*],
      {Dummy},
      {LogicalSeqScan},
      {LogicalInsert},
      {LogicalValues},
      {LogicalCreateTable},
      {LogicalDrop},
      {LogicalProjection},
      {LogicalFilter},
      {LogicalExplain},
      {LogicalJoin},
      {LogicalAggregate},
      {LogicalOrder},
      {LogicalLimit},
      {LogicalDelete},
      {LogicalCopyFromFile},
      {LogicalCopyToFile}
    }
  };
}

/// An enumeration which record all necessary information of execution plan,
/// which will be used by optimizer and executor.
macro_rules! logical_plan_enum {
  ([], $( { $node_name:ident } ),*) => {
    /// `ArrayImpl` embeds all possible array in `array` module.
    #[derive(Debug, PartialEq, Clone)]
    pub enum LogicalPlan {
      $( $node_name($node_name) ),*
    }
  };
}

for_all_plan_nodes! {logical_plan_enum}
pub(crate) type LogicalPlanRef = Rc<LogicalPlan>;

macro_rules! impl_plan_node {
    ([], $( { $node_name:ident } ),*) => {
        impl LogicalPlan {
            pub fn children(&self) -> Vec<LogicalPlanRef> {
                match self {
                    $( Self::$node_name(inner) => inner.children(),)*
                }
            }
            pub fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                match self {
                    $( Self::$node_name(inner) => inner.clone_with_children(children),)*
                }
            }
        }
  }
}
for_all_plan_nodes! { impl_plan_node }

pub(super) trait LeafLogicalPlanNode: Clone {}
macro_rules! impl_plan_node_for_leaf {
    ($leaf_node_type:ident) => {
        impl LogicalPlanNode for $leaf_node_type {
            fn children(&self) -> Vec<LogicalPlanRef> {
                vec![]
            }

            fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert!(children.is_empty());
                LogicalPlan::$leaf_node_type(self.clone()).into()
            }
        }
    };
}
pub(super) trait UnaryLogicalPlanNode {
    fn child(&self) -> LogicalPlanRef;
    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef;
}
macro_rules! impl_plan_node_for_unary {
    ($unary_node_type:ident) => {
        impl LogicalPlanNode for $unary_node_type {
            fn children(&self) -> Vec<LogicalPlanRef> {
                vec![self.child()]
            }

            fn clone_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert_eq!(children.len(), 1);
                self.clone_with_child(children.pop().unwrap())
            }
        }
    };
}

pub trait BinaryLogicalPlanNode {
    fn left(&self) -> LogicalPlanRef;
    fn right(&self) -> LogicalPlanRef;
    fn clone_with_left_right(&self, left: LogicalPlanRef, right: LogicalPlanRef) -> LogicalPlanRef;
}
macro_rules! impl_plan_node_for_binary {
    ($binary_node_type:ident) => {
        impl LogicalPlanNode for $binary_node_type {
            fn children(&self) -> Vec<LogicalPlanRef> {
                vec![self.left(), self.right()]
            }

            fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert_eq!(children.len(), 2);
                let mut iter = children.into_iter();
                self.clone_with_left_right(iter.next().unwrap(), iter.next().unwrap())
            }
        }
    };
}

impl_plan_node_for_leaf! {Dummy}
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
